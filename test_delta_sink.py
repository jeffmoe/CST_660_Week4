import re
import tempfile
import unittest
from datetime import date
from pathlib import Path

from deltalake import DeltaTable

from delta_sink import LATE_EVENTS_SCHEMA, DeltaSink, count_by, micros_to_date
from local_stream import EVENT_SCHEMA, US_PER_MINUTE

DAY0 = 1_790_553_600_000_000  # 2026-09-28T00:00:00Z in epoch micros


def window_row(minute: int, zone: str, n: int, batch_id: int, surge: float | None = None) -> dict:
    start = DAY0 + minute * US_PER_MINUTE
    return {"window_start": start, "window_end": start + 15 * US_PER_MINUTE, "zone": zone,
            "trip_count": n, "avg_surge_multiplier": surge,
            "finalized_at_watermark": start + 20 * US_PER_MINUTE,
            "batch_id": batch_id, "window_date": micros_to_date(start)}


def columns(dt: DeltaTable) -> list[str]:
    return [f.name for f in dt.schema().fields]


class DeltaSinkTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.sink = DeltaSink(Path(self._tmp.name))
        self.sink.ensure_tables()

    def tearDown(self):
        self._tmp.cleanup()

    def test_tables_are_created_partitioned_and_empty(self):
        for name, column in [("zone_window_counts", "window_date"), ("late_events", "event_date")]:
            dt = DeltaTable(self.sink.path(name))
            self.assertEqual(dt.metadata().partition_columns, [column])
            self.assertEqual(dt.to_pyarrow_table().num_rows, 0)
        self.sink.ensure_tables()  # second call is a no-op
        self.assertEqual(DeltaTable(self.sink.path("late_events")).version(), 0)

    def test_append_commits_batch_id_as_txn_version(self):
        self.assertTrue(self.sink.append("zone_window_counts", [window_row(0, "Z1", 3, 0)], "q1", 0))
        self.assertEqual(self.sink.committed_version("zone_window_counts", "q1"), 0)
        self.assertIsNone(self.sink.committed_version("zone_window_counts", "q2"))

    def test_replayed_batch_is_skipped(self):
        rows = [window_row(0, "Z1", 3, 0)]
        self.sink.append("zone_window_counts", rows, "q1", 0)
        self.sink.append("zone_window_counts", [window_row(15, "Z1", 1, 1)], "q1", 1)
        self.assertFalse(self.sink.append("zone_window_counts", rows, "q1", 0))
        self.assertFalse(self.sink.append("zone_window_counts", rows, "q1", 1))
        self.assertEqual(self.sink.read("zone_window_counts").num_rows, 2)

    def test_new_query_id_is_not_blocked_by_old_one(self):
        self.sink.append("zone_window_counts", [window_row(0, "Z1", 3, 5)], "old-query", 5)
        self.assertTrue(self.sink.append("zone_window_counts", [window_row(15, "Z1", 1, 0)], "new-query", 0))

    def test_time_travel_to_earlier_version(self):
        self.sink.append("zone_window_counts", [window_row(0, "Z1", 3, 0)], "q1", 0)
        self.sink.append("zone_window_counts", [window_row(15, "Z1", 1, 1)], "q1", 1)
        self.assertEqual(self.sink.read("zone_window_counts", version=1).num_rows, 1)
        self.assertEqual(self.sink.read("zone_window_counts").num_rows, 2)

    def test_compact_keeps_rows_and_txn(self):
        for b in range(4):
            self.sink.append("zone_window_counts", [window_row(15 * b, "Z1", b + 1, b)], "q1", b)
        self.sink.compact()
        self.assertEqual(len(DeltaTable(self.sink.path("zone_window_counts")).file_uris()), 1)
        self.assertEqual(self.sink.read("zone_window_counts").num_rows, 4)
        self.assertEqual(self.sink.committed_version("zone_window_counts", "q1"), 3)

    def test_count_by(self):
        self.sink.append("zone_window_counts", [window_row(0, "Z1", 1, 0), window_row(0, "Z2", 1, 0),
                                                 window_row(15, "Z1", 1, 0)], "q1", 0)
        self.assertEqual(count_by(self.sink.read("zone_window_counts"), ["zone"]),
                         [("Z1", 2), ("Z2", 1)])

    def test_tables_start_without_schema_v2_columns(self):
        self.assertNotIn("avg_surge_multiplier", columns(DeltaTable(self.sink.path("zone_window_counts"))))
        self.assertNotIn("surge_multiplier", columns(DeltaTable(self.sink.path("late_events"))))

    def test_all_null_new_column_does_not_evolve_schema(self):
        self.sink.append("zone_window_counts", [window_row(0, "Z1", 3, 0)], "q1", 0)
        self.assertNotIn("avg_surge_multiplier", columns(DeltaTable(self.sink.path("zone_window_counts"))))
        self.assertEqual(self.sink.schema_changes("zone_window_counts"), [])

    def test_first_value_evolves_schema_once(self):
        path = self.sink.path("zone_window_counts")
        self.sink.append("zone_window_counts", [window_row(0, "Z1", 3, 0)], "q1", 0)
        self.sink.append("zone_window_counts", [window_row(15, "Z1", 1, 1, surge=1.5)], "q1", 1)
        self.sink.append("zone_window_counts", [window_row(30, "Z1", 2, 2)], "q1", 2)

        changes = self.sink.schema_changes("zone_window_counts")
        self.assertEqual([(c["version"], c["change"]) for c in changes],
                         [(2, "added avg_surge_multiplier at batch 1")])
        self.assertNotIn("avg_surge_multiplier", columns(DeltaTable(path, version=1)))
        self.assertIn("avg_surge_multiplier", columns(DeltaTable(path, version=2)))
        surge = sorted(self.sink.read("zone_window_counts").to_pylist(), key=lambda r: r["window_start"])
        self.assertEqual([r["avg_surge_multiplier"] for r in surge], [None, 1.5, None])
        # the evolving commit is still exactly-once
        self.assertFalse(self.sink.append("zone_window_counts",
                                          [window_row(15, "Z1", 1, 1, surge=1.5)], "q1", 1))

    def test_micros_to_date_is_utc(self):
        self.assertEqual(micros_to_date(DAY0), date(2026, 9, 28))
        self.assertEqual(micros_to_date(DAY0 - 1), date(2026, 9, 27))


class SchemaParityTest(unittest.TestCase):
    def test_late_events_table_carries_every_input_column(self):
        input_columns = [m.group(1) for m in re.finditer(r"(\w+)\s+[A-Z]+", EVENT_SCHEMA)]
        self.assertEqual(input_columns, LATE_EVENTS_SCHEMA.names[:len(input_columns)])


if __name__ == "__main__":
    unittest.main()
