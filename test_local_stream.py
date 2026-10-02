import importlib.util
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from event_stream import generate_trip_events
from local_stream import (
    US_PER_MINUTE, LatenessPolicy, StateStore, WindowState, land_events,
)

POLICY = LatenessPolicy(window_minutes=15, allowed_lateness_minutes=10)
M = US_PER_MINUTE


class WindowStateTest(unittest.TestCase):
    def test_nothing_is_late_before_first_watermark(self):
        self.assertFalse(WindowState().is_late(0))

    def test_watermark_trails_max_event_time_and_never_regresses(self):
        state = WindowState()
        state.apply_batch([], 100 * M, POLICY)
        self.assertEqual(state.watermark_us, 90 * M)
        state.apply_batch([], 50 * M, POLICY)  # older batch must not pull it back
        self.assertEqual(state.watermark_us, 90 * M)

    def test_window_finalizes_once_when_watermark_passes_its_end(self):
        state = WindowState()
        self.assertEqual(state.apply_batch([(0, "Z1", 3)], 20 * M, POLICY), [])  # W=10 < end 15
        self.assertEqual(state.apply_batch([(0, "Z1", 2)], 25 * M, POLICY), [(0, "Z1", 5)])  # W=15
        self.assertEqual(state.open_counts, {})
        self.assertEqual(state.apply_batch([], 40 * M, POLICY), [])

    def test_late_window_cannot_be_counted_as_on_time(self):
        state = WindowState()
        state.apply_batch([], 60 * M, POLICY)  # W=50
        self.assertTrue(state.is_late(45 * M))
        self.assertFalse(state.is_late(60 * M))
        with self.assertRaises(ValueError):
            state.apply_batch([(30 * M, "Z1", 1)], 60 * M, POLICY)

    def test_json_round_trip(self):
        state = WindowState()
        state.apply_batch([(0, "Z1", 3), (15 * M, "Z2", 1)], 20 * M, POLICY)
        self.assertEqual(WindowState.from_json(state.to_json()), state)


class StateStoreTest(unittest.TestCase):
    def test_load_before_returns_previous_version(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = StateStore(Path(tmp))
            self.assertEqual(store.load_before(0), WindowState())
            for batch_id, max_t in [(0, 20), (1, 40), (2, 60)]:
                s = store.load_before(batch_id)
                s.apply_batch([], max_t * M, POLICY)
                store.save(batch_id, s)
            self.assertEqual(store.load_before(2).watermark_us, 30 * M)  # replay of 2 starts from 1
            self.assertEqual(store.latest().watermark_us, 50 * M)


class LandEventsTest(unittest.TestCase):
    def test_slices_preserve_arrival_order(self):
        with tempfile.TemporaryDirectory() as tmp:
            landing = Path(tmp)
            totals = land_events(generate_trip_events(1, 100), landing, slice_minutes=60)
            files = sorted(landing.glob("*.jsonl"))
            self.assertEqual(totals["files"], len(files))
            self.assertEqual(totals["events"], 200)
            mtimes = [f.stat().st_mtime for f in files]
            self.assertEqual(mtimes, sorted(mtimes))
            arrivals = [json.loads(line)["processing_time"]
                        for f in files for line in f.read_text().splitlines()]
            self.assertEqual(arrivals, sorted(arrivals))


@unittest.skipUnless(importlib.util.find_spec("pyspark"), "pyspark not installed")
class SparkPipelineTest(unittest.TestCase):
    """End-to-end on a small replay. Slow (~1 min): starts a local Spark session."""

    @classmethod
    def setUpClass(cls):
        from local_stream import build_spark, run_pipeline

        cls.tmp = Path(tempfile.mkdtemp())
        cls.out = cls.tmp / "out"
        cls.landed = land_events(generate_trip_events(1, 150, annotate=True),
                                 cls.out / "landing", slice_minutes=60)
        cls.spark = build_spark()
        cls.state = run_pipeline(cls.spark, cls.out / "landing", cls.out, POLICY)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def _read(self, name):
        return self.spark.read.parquet(str(self.out / name))

    def test_every_trip_is_counted_open_or_side_output(self):
        from local_stream import report
        summary = report(self.spark, self.out, self.landed, self.state)
        self.assertTrue(summary["reconciles"], summary)
        self.assertGreater(summary["late_side_output_events"], 0)

    def test_each_window_is_emitted_exactly_once(self):
        counts = self._read("zone_window_counts")
        self.assertEqual(counts.count(), counts.select("window_start", "zone").distinct().count())

    def test_outputs_respect_the_watermark(self):
        from pyspark.sql import functions as F
        counts = self._read("zone_window_counts")
        self.assertEqual(counts.filter(F.col("window_end") > F.col("finalized_at_watermark")).count(), 0)
        late = self._read("late_events")
        self.assertEqual(late.filter(F.col("window_end") > F.col("watermark")).count(), 0)
        self.assertEqual(late.filter(F.col("_delay_class") == "on_time").count(), 0)

    def test_replaying_a_batch_is_idempotent(self):
        from local_stream import EVENT_SCHEMA, StateStore, make_batch_processor
        out = self.tmp / "replay"
        store = StateStore(out / "_state")
        process = make_batch_processor(self.spark, out, POLICY, store)
        files = sorted((self.out / "landing").glob("*.jsonl"))[:12]
        batches = [self.spark.read.schema(EVENT_SCHEMA).json(str(f)) for f in files]
        for batch_id, df in enumerate(batches):
            process(df, batch_id)
        before = sorted(self.spark.read.parquet(str(out / "zone_window_counts")).collect())
        state_before = store.latest()

        process(batches[-1], len(batches) - 1)  # e.g. crash after sink write, before commit

        self.assertEqual(sorted(self.spark.read.parquet(str(out / "zone_window_counts")).collect()), before)
        self.assertEqual(store.latest(), state_before)

    def test_event_without_event_time_goes_to_side_output(self):
        from local_stream import EVENT_SCHEMA, StateStore, make_batch_processor
        out = self.tmp / "missing"
        bad = out / "bad.jsonl"
        bad.parent.mkdir(parents=True)
        bad.write_text(json.dumps({"event_id": "x", "event_type": "trip_started",
                                   "pickup_zone": "Z01-DOWNTOWN"}) + "\n")
        make_batch_processor(self.spark, out, POLICY, StateStore(out / "_state"))(
            self.spark.read.schema(EVENT_SCHEMA).json(str(bad)), 0)
        late = self.spark.read.parquet(str(out / "late_events")).collect()
        self.assertEqual([(r.event_id, r.late_reason) for r in late], [("x", "missing_event_time")])


if __name__ == "__main__":
    unittest.main()
