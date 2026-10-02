import tempfile
import unittest
from datetime import date
from pathlib import Path

from deltalake import DeltaTable

from delta_demo import (
    apply_bad_correction, busiest_late_day, last_version_before, main, schema_evolution,
    time_travel, zone_totals,
)
from delta_sink import DeltaSink, micros_to_date
from local_stream import US_PER_MINUTE

DAY0 = 1_790_553_600_000_000  # 2026-09-28T00:00:00Z
DAY_US = 24 * 60 * US_PER_MINUTE
DAY3, DAY4 = date(2026, 9, 30), date(2026, 10, 1)


def window(day: int, minute: int, zone: str, n: int, surge=None) -> dict:
    start = DAY0 + day * DAY_US + minute * US_PER_MINUTE
    return {"window_start": start, "window_end": start + 15 * US_PER_MINUTE, "zone": zone,
            "trip_count": n, "avg_surge_multiplier": surge,
            "finalized_at_watermark": start + 30 * US_PER_MINUTE, "batch_id": 0,
            "window_date": micros_to_date(start)}


def late_trip(day: int, minute: int, zone: str, surge=None) -> dict:
    t = DAY0 + day * DAY_US + minute * US_PER_MINUTE
    start = t - t % (15 * US_PER_MINUTE)
    return {"event_id": f"{zone}-{t}", "event_type": "trip_started", "event_time": t,
            "pickup_zone": zone, "surge_multiplier": surge, "late_reason": "beyond_watermark",
            "window_start": start, "window_end": start + 15 * US_PER_MINUTE,
            "event_date": micros_to_date(t)}


class DeltaDemoTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.out = Path(self._tmp.name)
        self.sink = DeltaSink(self.out / "delta")
        self.sink.ensure_tables()
        counts, late = "zone_window_counts", "late_events"
        # days 1-3 (schema v1), then day 4 brings surge (schema v2)
        self.sink.append(counts, [window(2, 0, "Z1", 10), window(2, 15, "Z2", 7)], "q", 0)
        self.sink.append(late, [late_trip(2, 5, "Z1"), late_trip(2, 20, "Z1")], "q", 0)
        self.sink.append(counts, [window(3, 0, "Z1", 4, surge=1.5)], "q", 1)
        self.sink.append(late, [late_trip(3, 3, "Z2", surge=2.0)], "q", 1)

    def tearDown(self):
        self._tmp.cleanup()

    def test_schema_evolution_report(self):
        r = schema_evolution(self.sink)
        counts = r["zone_window_counts"]
        self.assertEqual(counts["added"], ["avg_surge_multiplier"])
        self.assertEqual(counts["by_partition"][DAY3]["null"], 2)
        self.assertEqual(counts["by_partition"][DAY4]["value"], 1)
        self.assertEqual(r["late_events"]["added"], ["surge_multiplier"])

    def test_busiest_late_day(self):
        self.assertEqual(busiest_late_day(self.sink), DAY3)

    def test_bad_correction_then_time_travel_and_restore(self):
        before_totals = zone_totals(self.sink.read("zone_window_counts"), DAY3)
        bad = apply_bad_correction(self.sink, DAY3)

        # the correction replaced the day with late trips only, other days untouched
        self.assertEqual(zone_totals(self.sink.read("zone_window_counts"), DAY3), {"Z1": 2})
        self.assertEqual(zone_totals(self.sink.read("zone_window_counts"), DAY4), {"Z1": 4})

        found_bad, before = last_version_before(self.sink)
        self.assertEqual((found_bad, before), (bad, bad - 1))
        tt = time_travel(self.sink, DAY3, before)
        self.assertEqual(tt["before"], before_totals)
        self.assertEqual(tt["current"], {"Z1": 2})
        self.assertEqual(tt["timestamp_resolves_to"], before)

        DeltaTable(self.sink.path("zone_window_counts")).restore(before)
        self.assertEqual(zone_totals(self.sink.read("zone_window_counts"), DAY3), before_totals)

    def test_no_correction_found(self):
        with self.assertRaises(LookupError):
            last_version_before(self.sink)

    def test_cli_end_to_end(self):
        self.assertEqual(main(["--out", str(self.out)]), 0)
        self.assertEqual(zone_totals(self.sink.read("zone_window_counts"), DAY3), {"Z1": 10, "Z2": 7})


if __name__ == "__main__":
    unittest.main()
