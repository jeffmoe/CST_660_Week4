"""Schema evolution and time travel on the pipeline's Delta tables.

Run after ``local_stream.py`` has populated ``<out>/delta``::

    python delta_demo.py                  # evolution report, bad correction, time travel, restore
    python delta_demo.py --no-restore     # leave the bad correction in place to poke at

1. Schema evolution - shows the commit where ``surge_multiplier`` /
   ``avg_surge_multiplier`` were added when schema v2 data first arrived, the
   schema just before and after it, and that earlier rows read the column as null.
2. Bad late-data correction - replays last week's incident: a correction job
   recounts one day of zone windows from the late side output *only* and
   overwrites that day's partition, wiping the on-time counts.
3. Time travel - finds the last version before the correction from the Delta
   history, reads that snapshot (by version and by timestamp) and diffs it against
   the current table to prove what the day looked like before it was overwritten.
4. Restore - rolls the table back to that version (a new commit; nothing is lost).
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
from deltalake import CommitProperties, DeltaTable, write_deltalake

from delta_sink import TABLES, DeltaSink, micros_to_date

PROJECT_DIR = Path(__file__).resolve().parent
JOB_KEY = "tidewater.job"
BAD_CORRECTION = "late-data-correction"
COUNTS = "zone_window_counts"
PARTITION_COLUMN = {name: partition_by[0] for name, (_, partition_by, _) in TABLES.items()}


def _columns(dt: DeltaTable) -> list[str]:
    return [f.name for f in dt.schema().fields]


def _ms_to_datetime(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


# --------------------------------------------------------------------------
# 1. Schema evolution
# --------------------------------------------------------------------------

def schema_evolution(sink: DeltaSink) -> dict[str, dict]:
    """For each table: the evolving commit, columns before/after, null split by partition."""
    results = {}
    for name in TABLES:
        path = sink.path(name)
        changes = sink.schema_changes(name)
        if not changes:
            results[name] = {"changes": []}
            continue
        first = changes[0]["version"]
        before = _columns(DeltaTable(path, version=first - 1))
        after = _columns(DeltaTable(path, version=first))
        added = [c for c in after if c not in before]
        current = sink.read(name)
        part = PARTITION_COLUMN[name]
        split: dict[date, Counter] = {}
        for p, value in zip(current[part].to_pylist(), current[added[0]].to_pylist()):
            split.setdefault(p, Counter())["null" if value is None else "value"] += 1
        results[name] = {"changes": changes, "before": before, "after": after,
                         "added": added, "by_partition": split}
    return results


def print_schema_evolution(results: dict[str, dict]) -> None:
    print("== 1. Schema evolution ==")
    for name, r in results.items():
        if not r["changes"]:
            print(f"\n{name}: no schema change recorded (did the run reach day 4?)")
            continue
        c = r["changes"][0]
        print(f"\n{name}: v{c['version']} at {_ms_to_datetime(c['timestamp']):%Y-%m-%d %H:%M:%S} UTC"
              f" - {c['change']}")
        print(f"  columns at v{c['version'] - 1}: {', '.join(r['before'])}")
        print(f"  columns at v{c['version']}: {', '.join(r['after'])}")
        print(f"  {r['added'][0]} by {PARTITION_COLUMN[name]} (current table):")
        for part, counts in sorted(r["by_partition"].items(), key=lambda kv: str(kv[0])):
            print(f"    {str(part):12} null {counts['null']:>5}   value {counts['value']:>5}")


# --------------------------------------------------------------------------
# 2. Bad correction
# --------------------------------------------------------------------------

def busiest_late_day(sink: DeltaSink) -> date | None:
    late = sink.read("late_events")
    trips = late.filter(pc.equal(late["event_type"], "trip_started"))
    days = Counter(d for d in trips["event_date"].to_pylist() if d is not None)
    return days.most_common(1)[0][0] if days else None


def late_trips_on(sink: DeltaSink, day: date) -> pa.Table:
    late = sink.read("late_events")
    return late.filter(pc.and_(pc.equal(late["event_type"], "trip_started"),
                               pc.equal(late["event_date"], pa.scalar(day, pa.date32()))))


def apply_bad_correction(sink: DeltaSink, day: date) -> int:
    """Recount ``day`` from late events only and overwrite that partition - the bug.

    The right fix would add late trips to the published counts (or keep them in a
    separate adjustments table); replacing the partition throws away every on-time
    trip for the day. Returns the version of the bad commit."""
    trips = late_trips_on(sink, day)
    recount = Counter(zip(trips["window_start"].cast(pa.int64()).to_pylist(),
                          trips["window_end"].cast(pa.int64()).to_pylist(),
                          trips["pickup_zone"].to_pylist()))
    now_us = int(datetime.now(timezone.utc).timestamp() * 1_000_000)
    dt = DeltaTable(sink.path(COUNTS))
    columns = set(_columns(dt))
    schema = pa.schema([f for f in TABLES[COUNTS][0] if f.name in columns])
    rows = [{"window_start": s, "window_end": e, "zone": z, "trip_count": n,
             "avg_surge_multiplier": None, "finalized_at_watermark": now_us,
             "batch_id": -1, "window_date": micros_to_date(s)}
            for (s, e, z), n in sorted(recount.items())]
    write_deltalake(
        dt, pa.Table.from_pylist(rows, schema=schema), mode="overwrite",
        predicate=f"window_date = '{day.isoformat()}'",
        commit_properties=CommitProperties(custom_metadata={JOB_KEY: BAD_CORRECTION}),
    )
    return DeltaTable(sink.path(COUNTS)).version()


# --------------------------------------------------------------------------
# 3. Time travel
# --------------------------------------------------------------------------

def last_version_before(sink: DeltaSink, job: str = BAD_CORRECTION) -> tuple[int, int]:
    """(bad_version, version_before) for the most recent commit tagged with ``job``."""
    tagged = [h for h in DeltaTable(sink.path(COUNTS)).history() if h.get(JOB_KEY) == job]
    if not tagged:
        raise LookupError(f"no commit tagged {JOB_KEY}={job!r} in {COUNTS}")
    bad = max(h["version"] for h in tagged)
    return bad, bad - 1


def zone_totals(table: pa.Table, day: date) -> dict[str, int]:
    rows = table.filter(pc.equal(table["window_date"], pa.scalar(day, pa.date32())))
    totals: Counter = Counter()
    for zone, n in zip(rows["zone"].to_pylist(), rows["trip_count"].to_pylist()):
        totals[zone] += n
    return dict(totals)


def time_travel(sink: DeltaSink, day: date, version_before: int) -> dict:
    path = sink.path(COUNTS)
    by_version = DeltaTable(path, version=version_before)
    commit_ms = next(h["timestamp"] for h in DeltaTable(path).history()
                     if h["version"] == version_before)
    by_timestamp = DeltaTable(path)
    by_timestamp.load_as_version(_ms_to_datetime(commit_ms))
    return {
        "version_before": version_before,
        "timestamp_resolves_to": by_timestamp.version(),
        "before": zone_totals(by_version.to_pyarrow_table(), day),
        "current": zone_totals(DeltaTable(path).to_pyarrow_table(), day),
    }


def print_time_travel(day: date, bad_version: int, tt: dict) -> None:
    print(f"\n== 3. Time travel: {COUNTS} for {day} ==")
    print(f"bad correction committed as v{bad_version}; last good snapshot is "
          f"v{tt['version_before']} (by timestamp -> v{tt['timestamp_resolves_to']})")
    print(f"  {'zone':20} {'v' + str(tt['version_before']):>8} {'current':>8} {'lost':>6}")
    for zone in sorted(set(tt["before"]) | set(tt["current"])):
        b, c = tt["before"].get(zone, 0), tt["current"].get(zone, 0)
        print(f"  {zone:20} {b:>8} {c:>8} {b - c:>6}")
    b, c = sum(tt["before"].values()), sum(tt["current"].values())
    print(f"  {'TOTAL':20} {b:>8} {c:>8} {b - c:>6}")


# --------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=PROJECT_DIR / "stream_output")
    parser.add_argument("--day", type=date.fromisoformat, default=None,
                        help="window_date to corrupt (default: day with the most late trips)")
    parser.add_argument("--no-restore", action="store_true",
                        help="leave the bad correction in place")
    args = parser.parse_args(argv)

    sink = DeltaSink(args.out / "delta")
    if not all(DeltaTable.is_deltatable(sink.path(n)) for n in TABLES):
        sys.exit(f"No Delta tables under {sink.root}; run local_stream.py first")

    print_schema_evolution(schema_evolution(sink))

    day = args.day or busiest_late_day(sink)
    if day is None:
        sys.exit("No late trips to build a correction from")
    late_trips = late_trips_on(sink, day).num_rows
    print(f"\n== 2. Bad late-data correction on {day} ==")
    bad_version = apply_bad_correction(sink, day)
    print(f"overwrote partition window_date={day} with {late_trips} late trips "
          f"(commit v{bad_version}, tagged {JOB_KEY}={BAD_CORRECTION})")

    bad, before = last_version_before(sink)
    tt = time_travel(sink, day, before)
    print_time_travel(day, bad, tt)

    if args.no_restore:
        print(f"\nLeft v{bad} in place. Restore later with:  DeltaTable(path).restore({before})")
        return 0
    print("\n== 4. Restore ==")
    dt = DeltaTable(sink.path(COUNTS))
    metrics = dt.restore(before)
    restored = DeltaTable(sink.path(COUNTS))
    ok = zone_totals(restored.to_pyarrow_table(), day) == tt["before"]
    print(f"restored to v{before} as new commit v{restored.version()} "
          f"({metrics.get('numRestoredFile', '?')} files restored); day matches v{before}: {ok}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
