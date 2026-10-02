"""Local Spark Structured Streaming job: trips per zone in tumbling event-time windows.

Pipeline::

    event_stream.generate_trip_events()        (phones, in arrival order)
        -> landing/*.jsonl                     (one file per processing-time slice)
        -> Spark file source, 1 file/trigger   (one micro-batch per slice)
        -> foreachBatch(process_batch)
              |- delta/zone_window_counts      Delta table: finalized windows, written once
              |- delta/late_events             Delta table: side output beyond the watermark
              '- _state/<batch_id>.json        watermark + open windows (for restarts)

Results land in Delta Lake via delta-rs (see ``delta_sink.py``); each micro-batch
commit carries a Delta txn(appId=query id, version=batch id), so replays are no-ops.

Why the watermark is explicit instead of ``withWatermark``: Spark's built-in
watermark silently drops late rows and offers no side output for them, which is
exactly how surge analytics ended up undercounting tunnel trips. Here Spark still
assigns the tumbling windows and does the counting, but the lateness policy
(``LatenessPolicy`` + ``WindowState``) is ours, so every input event lands in
exactly one place: a finalized count, a still-open window, or the late side output.

Lateness policy (same semantics as Spark's watermark):

* watermark W = max(event_time seen in *previous* micro-batches) - allowed_lateness,
  and it never moves backwards.
* An event is late iff the tumbling window it falls in ends at or before W, i.e.
  that window has already been finalized. Late events go to ``late_events``
  with the watermark they missed, never into the counts.
* After each batch W advances; every open window whose end <= W is finalized and
  written once to ``zone_window_counts``. Finalized rows are never rewritten, so
  late data cannot overwrite published aggregates; corrections must be additive
  from the side output.

Run:  python local_stream.py --reset --days 5
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, Iterator

import pyarrow.compute as pc

from delta_sink import DeltaSink, checkpoint_query_id, count_by, micros_to_date, total
from event_stream import generate_trip_events

PROJECT_DIR = Path(__file__).resolve().parent
US_PER_MINUTE = 60 * 1_000_000

EVENT_SCHEMA = """
    event_id STRING, event_type STRING, schema_version INT,
    event_time TIMESTAMP, processing_time TIMESTAMP,
    trip_id STRING, driver_id STRING, pickup_zone STRING, dropoff_zone STRING,
    distance_km DOUBLE, fare_usd DOUBLE, surge_multiplier DOUBLE,
    _delay_class STRING, _delay_seconds DOUBLE
"""
# surge_multiplier only exists from simulated day 4 (schema v2); declaring the
# superset schema lets v1 rows read it as null instead of breaking the stream.
# _delay_* are generator ground truth for reporting and are null in a real feed.


# --------------------------------------------------------------------------
# Lateness policy and window state (pure Python, no Spark needed)
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class LatenessPolicy:
    window_minutes: int = 15
    allowed_lateness_minutes: int = 10

    @property
    def window_us(self) -> int:
        return self.window_minutes * US_PER_MINUTE

    @property
    def lateness_us(self) -> int:
        return self.allowed_lateness_minutes * US_PER_MINUTE


@dataclass
class WindowState:
    """Watermark and open-window counts carried between micro-batches.

    Times are epoch microseconds (UTC) to avoid Python/Spark timezone conversion.
    """

    max_event_time_us: int | None = None
    watermark_us: int | None = None  # None until the first batch: nothing is late yet
    open_counts: dict[tuple[int, str], int] = field(default_factory=dict)  # (window_start, zone)

    def is_late(self, window_end_us: int) -> bool:
        return self.watermark_us is not None and window_end_us <= self.watermark_us

    def apply_batch(
        self,
        on_time_counts: Iterable[tuple[int, str, int]],
        batch_max_event_time_us: int | None,
        policy: LatenessPolicy,
    ) -> list[tuple[int, str, int]]:
        """Merge one batch's on-time counts, advance the watermark, and return the
        (window_start_us, zone, count) rows that are now final."""
        for window_start, zone, n in on_time_counts:
            if self.is_late(window_start + policy.window_us):
                raise ValueError(f"late window {window_start}/{zone} passed as on-time")
            key = (window_start, zone)
            self.open_counts[key] = self.open_counts.get(key, 0) + n

        if batch_max_event_time_us is not None:
            if self.max_event_time_us is None or batch_max_event_time_us > self.max_event_time_us:
                self.max_event_time_us = batch_max_event_time_us
            candidate = self.max_event_time_us - policy.lateness_us
            if self.watermark_us is None or candidate > self.watermark_us:
                self.watermark_us = candidate

        finalized = sorted(
            (start, zone, n) for (start, zone), n in self.open_counts.items()
            if self.is_late(start + policy.window_us)
        )
        for start, zone, _ in finalized:
            del self.open_counts[(start, zone)]
        return finalized

    def to_json(self) -> str:
        return json.dumps({
            "max_event_time_us": self.max_event_time_us,
            "watermark_us": self.watermark_us,
            "open_counts": [[s, z, n] for (s, z), n in sorted(self.open_counts.items())],
        })

    @classmethod
    def from_json(cls, text: str) -> "WindowState":
        raw = json.loads(text)
        return cls(raw["max_event_time_us"], raw["watermark_us"],
                   {(s, z): n for s, z, n in raw["open_counts"]})


class StateStore:
    """One JSON snapshot per batch id, so a replayed batch N restarts from N-1."""

    def __init__(self, directory: Path):
        self.directory = directory
        directory.mkdir(parents=True, exist_ok=True)

    def _versions(self) -> list[int]:
        return sorted(int(p.stem) for p in self.directory.glob("*.json"))

    def load_before(self, batch_id: int) -> WindowState:
        earlier = [v for v in self._versions() if v < batch_id]
        if not earlier:
            return WindowState()
        return WindowState.from_json((self.directory / f"{earlier[-1]}.json").read_text())

    def latest(self) -> WindowState:
        versions = self._versions()
        return self.load_before(versions[-1] + 1) if versions else WindowState()

    def save(self, batch_id: int, state: WindowState) -> None:
        tmp = self.directory / f"{batch_id}.json.tmp"
        tmp.write_text(state.to_json())
        os.replace(tmp, self.directory / f"{batch_id}.json")


# --------------------------------------------------------------------------
# Landing zone: replay the generator as processing-time slices
# --------------------------------------------------------------------------

def land_events(events: Iterator[dict], landing_dir: Path, slice_minutes: int) -> Counter:
    """Write events into one JSONL file per processing-time slice.

    Files get strictly increasing mtimes so Spark's file source (which orders by
    modification time) replays them in arrival order, one slice per micro-batch.
    """
    landing_dir.mkdir(parents=True, exist_ok=True)
    slice_us = slice_minutes * US_PER_MINUTE
    totals: Counter = Counter()
    base_mtime = datetime.now().timestamp() - 1_000_000
    current_slice, buffer, file_index = None, [], 0

    def flush():
        nonlocal file_index
        if not buffer:
            return
        path = landing_dir / f"events-{file_index:05d}.jsonl"
        path.write_text("".join(buffer), encoding="utf-8")
        os.utime(path, (base_mtime + file_index, base_mtime + file_index))
        file_index += 1
        buffer.clear()

    for event in events:
        arrived = datetime.fromisoformat(event["processing_time"].replace("Z", "+00:00"))
        slice_id = int(arrived.timestamp() * 1_000_000) // slice_us
        if slice_id != current_slice:
            flush()
            current_slice = slice_id
        buffer.append(json.dumps(event) + "\n")
        totals["events"] += 1
        totals[event["event_type"]] += 1
    flush()
    totals["files"] = file_index
    return totals


# --------------------------------------------------------------------------
# Spark
# --------------------------------------------------------------------------

def _configure_local_environment() -> None:
    """Point Spark at this venv's Python and, on Windows, the bundled winutils."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    import pyspark  # pin SPARK_HOME; auto-detection breaks on paths with spaces on Windows
    os.environ.setdefault("SPARK_HOME", str(Path(pyspark.__file__).parent))
    hadoop_home = PROJECT_DIR / "hadoop"
    if os.name == "nt" and "HADOOP_HOME" not in os.environ and (hadoop_home / "bin" / "winutils.exe").exists():
        os.environ["HADOOP_HOME"] = str(hadoop_home)
        os.environ["PATH"] = f"{hadoop_home / 'bin'}{os.pathsep}{os.environ['PATH']}"


def build_spark():
    _configure_local_environment()
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[4]")
        .appName("tidewater-zone-windows")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def make_batch_processor(sink: DeltaSink, policy: LatenessPolicy, store: StateStore,
                         app_id: Callable[[], str]):
    """``app_id`` names the writer in Delta txn actions (the query id); it is a
    callable because Spark only persists the query id once the query has started."""
    from pyspark.sql import functions as F

    def process_batch(batch_df, batch_id: int) -> None:
        writer = app_id()
        state = store.load_before(batch_id)
        watermark = state.watermark_us

        # Tumbling window bounds as arithmetic on epoch micros. Deliberately not
        # F.window(): it inserts a hidden isnotnull(event_time) filter, which would
        # silently drop events with no event_time instead of side-outputting them.
        event_us = F.unix_micros("event_time")
        df = (
            batch_df
            .withColumn("window_start_us", event_us - F.pmod(event_us, F.lit(policy.window_us)))
            .withColumn("window_end_us", F.col("window_start_us") + F.lit(policy.window_us))
            .withColumn(
                "late_reason",
                F.when(F.col("event_time").isNull(), F.lit("missing_event_time"))
                 .when(F.lit(watermark).isNotNull() & (F.col("window_end_us") <= F.lit(watermark)),
                       F.lit("beyond_watermark")),
            )
        )

        # One small Spark job per batch: counts per (window, zone, type, lateness).
        # The result is bounded by windows x zones, so collecting it is cheap.
        groups = (
            df.groupBy("window_start_us", F.col("pickup_zone").alias("zone"),
                       "event_type", "late_reason")
            .agg(F.count("*").alias("n"), F.max(event_us).alias("max_us"))
            .collect()
        )
        late_rows = sum(g.n for g in groups if g.late_reason is not None)
        batch_max = max((g.max_us for g in groups if g.max_us is not None), default=None)
        on_time_counts = [
            (g.window_start_us, g.zone, g.n) for g in groups
            if g.late_reason is None and g.event_type == "trip_started"
        ]

        # Side output first: everything the watermark rejects is kept, not dropped.
        if late_rows:
            timestamps = {c for c, t in batch_df.dtypes if t == "timestamp"}
            late = df.filter(F.col("late_reason").isNotNull()).select(
                # Timestamps cross into Python as epoch micros: collect() would turn
                # them into naive local-time datetimes.
                *[F.unix_micros(c).alias(c) if c in timestamps else F.col(c)
                  for c in batch_df.columns],
                "late_reason",
                F.col("window_start_us").alias("window_start"),
                F.col("window_end_us").alias("window_end"),
                F.lit(watermark).cast("long").alias("watermark"),
                F.lit(batch_id).cast("long").alias("batch_id"),
                F.to_date("event_time").alias("event_date"),
            )
            sink.append("late_events", [r.asDict() for r in late.collect()], writer, batch_id)

        finalized = state.apply_batch(on_time_counts, batch_max, policy)
        if finalized:
            sink.append("zone_window_counts", [
                {"window_start": start, "window_end": start + policy.window_us, "zone": zone,
                 "trip_count": n, "finalized_at_watermark": state.watermark_us,
                 "batch_id": batch_id, "window_date": micros_to_date(start)}
                for start, zone, n in finalized
            ], writer, batch_id)

        # Commit state last: a crash before this replays the batch from N-1, and
        # the Delta txn check in sink.append makes that replay a no-op.
        store.save(batch_id, state)

    return process_batch


def run_pipeline(spark, landing_dir: Path, out_dir: Path, policy: LatenessPolicy,
                 sink: DeltaSink) -> WindowState:
    store = StateStore(out_dir / "_state")
    checkpoint = out_dir / "_checkpoint"
    sink.ensure_tables()
    events = (
        spark.readStream.schema(EVENT_SCHEMA)
        .option("maxFilesPerTrigger", 1)
        .json(str(landing_dir))
    )
    query = (
        events.writeStream
        .queryName("zone_window_counts")
        .foreachBatch(make_batch_processor(
            sink, policy, store, lambda: f"zone-windows-{checkpoint_query_id(checkpoint)}"))
        .option("checkpointLocation", str(checkpoint))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    return store.latest()


def report(sink: DeltaSink, landed: Counter, state: WindowState) -> dict:
    counts, late = sink.read("zone_window_counts"), sink.read("late_events")
    late_trips = late.filter(pc.equal(late["event_type"], "trip_started"))
    open_trips = sum(state.open_counts.values())
    summary = {
        "landed_events": landed["events"],
        "micro_batches": landed["files"],
        "landed_trips": landed["trip_started"],
        "finalized_window_trips": total(counts, "trip_count"),
        "open_window_trips": open_trips,
        "late_side_output_trips": late_trips.num_rows,
        "late_side_output_events": late.num_rows,
        "watermark": None if state.watermark_us is None else
            (datetime(1970, 1, 1, tzinfo=timezone.utc)
             + timedelta(microseconds=state.watermark_us)).isoformat(),
    }
    summary["reconciles"] = (summary["finalized_window_trips"] + open_trips
                             + late_trips.num_rows == summary["landed_trips"])

    print("\n== Pipeline summary ==")
    for key, value in summary.items():
        print(f"{key:26}: {value:,}" if isinstance(value, int) and not isinstance(value, bool)
              else f"{key:26}: {value}")
    print("\nLate side output by reason / injected delay class:")
    for reason, delay_class, n in count_by(late, ["late_reason", "_delay_class"]):
        print(f"  {reason:20} {delay_class or '-':12} {n:>6,}")
    print("Late trips by pickup zone:")
    for zone, n in count_by(late_trips, ["pickup_zone"]):
        print(f"  {zone:20} {n:>6,}")
    print("\nDelta tables:")
    for name, info in sink.describe().items():
        print(f"  {name:20} version {info['version']:>4}  files {info['files']:>4}  "
              f"rows {info['rows']:>6,}  ops {info['operations']}")
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Tumbling event-time windows of trips per zone.")
    parser.add_argument("--days", type=int, default=5, help="simulated days (schema v2 starts day 4)")
    parser.add_argument("--trips-per-day", type=int, default=1_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--slice-minutes", type=int, default=15,
                        help="processing-time span of each micro-batch")
    parser.add_argument("--window-minutes", type=int, default=15)
    parser.add_argument("--allowed-lateness-minutes", type=int, default=10)
    parser.add_argument("--out", type=Path, default=PROJECT_DIR / "stream_output")
    parser.add_argument("--reset", action="store_true",
                        help="delete --out (landing, Delta tables, checkpoint) before running")
    parser.add_argument("--skip-compact", action="store_true",
                        help="don't OPTIMIZE (bin-pack) the Delta tables after the run")
    args = parser.parse_args(argv)

    policy = LatenessPolicy(args.window_minutes, args.allowed_lateness_minutes)
    landing_dir = args.out / "landing"
    if args.reset and args.out.exists():
        shutil.rmtree(args.out)
    if landing_dir.exists():
        sys.exit(f"{landing_dir} already exists; rerun with --reset to start over")

    landed = land_events(
        generate_trip_events(args.days, args.trips_per_day, seed=args.seed, annotate=True),
        landing_dir, args.slice_minutes,
    )
    print(f"Landed {landed['events']:,} events in {landed['files']} slice files "
          f"({args.slice_minutes} min each) under {landing_dir}")
    print(f"Policy: {policy.window_minutes}-min tumbling windows, "
          f"{policy.allowed_lateness_minutes}-min allowed lateness")

    sink = DeltaSink(args.out / "delta")
    spark = build_spark()
    try:
        state = run_pipeline(spark, landing_dir, args.out, policy, sink)
    finally:
        spark.stop()
    if not args.skip_compact:
        for name, m in sink.compact().items():
            print(f"OPTIMIZE {name}: {m['numFilesRemoved']} files -> {m['numFilesAdded']}")
    summary = report(sink, landed, state)
    return 0 if summary["reconciles"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
