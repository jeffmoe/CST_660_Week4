# CST_660_Week4
Repo For Streaming and Open Table Format Prototype

## Event stream generator (`event_stream.py`)

Synthetic Tidewater trip events with separate `event_time` / `processing_time`,
emitted in arrival order with injected out-of-order, late (harbor-tunnel
buffering) and very-late events, plus a schema change on simulated day 4
(`surge_multiplier`, `schema_version` 2). Standard library only.

```
python event_stream.py --summary --out events.jsonl
```

## Local Spark Structured Streaming job (`local_stream.py`)

Counts trips per pickup zone in tumbling event-time windows with an explicit
watermark policy, and routes events beyond the watermark to a side output
instead of dropping them.

```
generator -> stream_output/landing/*.jsonl (one file per processing-time slice)
          -> Spark file source, 1 file per micro-batch -> foreachBatch
               zone_window_counts/   finalized windows, each emitted exactly once
               late_events/          side output: events beyond the watermark
               _state/<batch>.json   watermark + open windows (restart safe)
```

Policy (defaults: 15-min windows, 10-min allowed lateness):

* watermark = max event_time seen in previous micro-batches - allowed lateness (never regresses)
* an event is **late** if its window ends at or before the watermark; it goes to
  `late_events/` with the watermark it missed and a `late_reason`
* a window is finalized and written once when the watermark passes its end;
  finalized rows are never rewritten, so late data can't overwrite published aggregates

Spark's built-in `withWatermark` drops late rows silently with no side output,
so the policy is applied explicitly in `foreachBatch`; Spark still assigns the
windows and does the counting. The run ends with a reconciliation check:
finalized + still-open + late trips must equal landed trips.

### Setup

Requires Java 8/11/17 (PySpark 3.5 is the last line that supports Java 8).

```
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
```

On Windows, Spark also needs Hadoop's native `winutils.exe` and `hadoop.dll`.
Put the Hadoop 3.3.x builds (e.g. from the community repo
https://github.com/cdarlint/winutils, `hadoop-3.3.6/bin`) in `./hadoop/bin`
(gitignored); `local_stream.py` sets `HADOOP_HOME` to it automatically.

### Run

```
.venv\Scripts\python local_stream.py --reset            # 5 days, 1000 trips/day
.venv\Scripts\python local_stream.py --help              # window, lateness, slice size
.venv\Scripts\python -m unittest -v                      # includes a ~1 min Spark test
```
