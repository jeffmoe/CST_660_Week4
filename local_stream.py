"""Local Spark Structured Streaming job: trips per zone in tumbling event-time windows.

Pipeline::

    event_stream.generate_trip_events()        (phones, in arrival order)
        -> landing/*.jsonl                     (one file per processing-time slice)
        -> Spark file source, 1 file/trigger   (one micro-batch per slice)
        -> foreachBatch(process_batch)
              |- zone_window_counts/           finalized windows, emitted exactly once
              |- late_events/                  side output: events beyond the watermark
              '- _state/<batch_id>.json        watermark + open windows (for restarts)

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
  that window has already been finalized. Late events go to ``late_events/``
  with the watermark they missed, never into the counts.
* After each batch W advances; every open window whose end <= W is finalized and
  written once to ``zone_window_counts/``. Finalized rows are never rewritten, so
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
from typing import Iterable, Iterator

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


def make_batch_processor(spark, out_dir: Path, policy: LatenessPolicy, store: StateStore):
    from pyspark.sql import functions as F

    counts_dir = out_dir / "zone_window_counts"
    late_dir = out_dir / "late_events"

    def process_batch(batch_df, batch_id: int) -> None:
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
            (df.filter(F.col("late_reason").isNotNull())
               .withColumn("window_start", F.timestamp_micros("window_start_us"))
               .withColumn("window_end", F.timestamp_micros("window_end_us"))
               .withColumn("watermark", F.expr(f"timestamp_micros({watermark})")
                           if watermark is not None else F.lit(None).cast("timestamp"))
               .drop("window_start_us", "window_end_us")
               .coalesce(1)
               .write.mode("overwrite").parquet(str(late_dir / f"batch_id={batch_id}")))

        finalized = state.apply_batch(on_time_counts, batch_max, policy)
        if finalized:
            # Built from literals rather than spark.createDataFrame(rows): on Windows
            # PySpark can't reuse Python workers, so a Python-row DataFrame costs ~2 s
            # per write to spawn one; this stays in the JVM (~0.05 s).
            rows = [F.struct(F.lit(start).cast("long").alias("window_start_us"),
                             F.lit(zone).alias("zone"),
                             F.lit(n).cast("long").alias("trip_count"))
                    for start, zone, n in finalized]
            (spark.range(1).select(F.inline(F.array(*rows)))
               .select(F.expr("timestamp_micros(window_start_us)").alias("window_start"),
                       F.expr(f"timestamp_micros(window_start_us + {policy.window_us})")
                        .alias("window_end"),
                       "zone", "trip_count",
                       F.expr(f"timestamp_micros({state.watermark_us})").alias("finalized_at_watermark"))
               .coalesce(1)
               .write.mode("overwrite").parquet(str(counts_dir / f"batch_id={batch_id}")))

        # Commit state last: a crash before this replays the batch from N-1, and
        # the overwrite-by-batch_id sinks above make that replay idempotent.
        store.save(batch_id, state)

    return process_batch


def run_pipeline(spark, landing_dir: Path, out_dir: Path, policy: LatenessPolicy) -> WindowState:
    store = StateStore(out_dir / "_state")
    events = (
        spark.readStream.schema(EVENT_SCHEMA)
        .option("maxFilesPerTrigger", 1)
        .json(str(landing_dir))
    )
    query = (
        events.writeStream
        .queryName("zone_window_counts")
        .foreachBatch(make_batch_processor(spark, out_dir, policy, store))
        .option("checkpointLocation", str(out_dir / "_checkpoint"))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    return store.latest()


def report(spark, out_dir: Path, landed: Counter, state: WindowState) -> dict:
    from pyspark.sql import functions as F

    def read(name):
        path = out_dir / name
        return spark.read.parquet(str(path)) if path.exists() else None

    counts, late = read("zone_window_counts"), read("late_events")
    finalized_trips = counts.agg(F.sum("trip_count")).first()[0] if counts else 0
    open_trips = sum(state.open_counts.values())
    late_trips = late.filter(F.col("event_type") == "trip_started").count() if late else 0
    summary = {
        "landed_events": landed["events"],
        "micro_batches": landed["files"],
        "landed_trips": landed["trip_started"],
        "finalized_window_trips": finalized_trips or 0,
        "open_window_trips": open_trips,
        "late_side_output_trips": late_trips,
        "late_side_output_events": late.count() if late else 0,
        "watermark": None if state.watermark_us is None else
            (datetime(1970, 1, 1, tzinfo=timezone.utc)
             + timedelta(microseconds=state.watermark_us)).isoformat(),
    }
    summary["reconciles"] = (summary["finalized_window_trips"] + open_trips + late_trips
                             == summary["landed_trips"])

    print("\n== Pipeline summary ==")
    for key, value in summary.items():
        print(f"{key:26}: {value:,}" if isinstance(value, int) and not isinstance(value, bool)
              else f"{key:26}: {value}")
    if late is not None:
        print("\nLate side output by reason / injected delay class:")
        late.groupBy("late_reason", "_delay_class").count().orderBy("late_reason", "_delay_class").show()
        print("Late trips by pickup zone:")
        (late.filter(F.col("event_type") == "trip_started")
             .groupBy("pickup_zone").count().orderBy(F.desc("count")).show())
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
                        help="delete --out (landing, sinks, checkpoint) before running")
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

    spark = build_spark()
    try:
        state = run_pipeline(spark, landing_dir, args.out, policy)
        summary = report(spark, args.out, landed, state)
    finally:
        spark.stop()
    return 0 if summary["reconciles"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
