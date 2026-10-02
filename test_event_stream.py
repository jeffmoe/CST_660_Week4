import unittest
from collections import Counter
from datetime import datetime, timedelta

from event_stream import SCHEMA_CHANGE_DAY, generate_trip_events


def _ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class EventStreamTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.events = list(generate_trip_events(num_days=6, trips_per_day=500, annotate=True))
        cls.start = datetime.fromisoformat("2026-09-28T00:00:00+00:00")

    def test_every_trip_has_start_and_end(self):
        per_trip = Counter(e["trip_id"] for e in self.events)
        self.assertEqual(len(per_trip), 6 * 500)
        self.assertTrue(all(n == 2 for n in per_trip.values()))

    def test_emitted_in_processing_time_order(self):
        times = [e["processing_time"] for e in self.events]
        self.assertEqual(times, sorted(times))

    def test_processing_never_precedes_event_time(self):
        for e in self.events:
            self.assertGreaterEqual(_ts(e["processing_time"]), _ts(e["event_time"]))

    def test_out_of_order_and_late_events_present(self):
        out_of_order = 0
        high_water = ""
        for e in self.events:
            if e["event_time"] < high_water:
                out_of_order += 1
            high_water = max(high_water, e["event_time"])
        self.assertGreater(out_of_order, 0)
        classes = Counter(e["_delay_class"] for e in self.events)
        for name in ("on_time", "out_of_order", "late", "very_late"):
            self.assertGreater(classes[name], 0, name)

    def test_tunnel_zones_are_late_more_often(self):
        def late_rate(tunnel):
            subset = [e for e in self.events
                      if ("TUNNEL" in (e["pickup_zone"] if e["event_type"] == "trip_started"
                                       else e["dropoff_zone"])) == tunnel]
            return sum(e["_delay_class"] == "late" for e in subset) / len(subset)
        self.assertGreater(late_rate(True), 3 * late_rate(False))

    def test_schema_change_on_day_four(self):
        boundary = self.start + timedelta(days=SCHEMA_CHANGE_DAY - 1)
        for e in self.events:
            if _ts(e["event_time"]) < boundary:
                self.assertEqual(e["schema_version"], 1)
                self.assertNotIn("surge_multiplier", e)
            elif e["event_type"] == "trip_started":
                self.assertEqual(e["schema_version"], 2)
                self.assertIn("surge_multiplier", e)

    def test_schemas_interleave_in_arrival_order(self):
        first_v2 = next(i for i, e in enumerate(self.events) if e["schema_version"] == 2)
        self.assertTrue(any(e["schema_version"] == 1 for e in self.events[first_v2:]))

    def test_deterministic_for_seed(self):
        a = list(generate_trip_events(num_days=1, trips_per_day=50, seed=7))
        b = list(generate_trip_events(num_days=1, trips_per_day=50, seed=7))
        self.assertEqual(a, b)

    def test_annotations_hidden_by_default(self):
        event = next(generate_trip_events(num_days=1, trips_per_day=10))
        self.assertFalse(any(k.startswith("_") for k in event))


if __name__ == "__main__":
    unittest.main()
