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
               delta/zone_window_counts   Delta table: finalized windows, each written once
               delta/late_events          Delta table: side output beyond the watermark
               _state/<batch>.json        watermark + open windows (restart safe)
```

Policy (defaults: 15-min windows, 10-min allowed lateness):

* watermark = max event_time seen in previous micro-batches - allowed lateness (never regresses)
* an event is **late** if its window ends at or before the watermark; it goes to
  `late_events` with the watermark it missed and a `late_reason`
* a window is finalized and written once when the watermark passes its end;
  finalized rows are never rewritten, so late data can't overwrite published aggregates

Spark's built-in `withWatermark` drops late rows silently with no side output,
so the policy is applied explicitly in `foreachBatch`; Spark still assigns the
windows and does the counting. The run ends with a reconciliation check:
finalized + still-open + late trips must equal landed trips.

### Delta Lake tables (`delta_sink.py`)

Results land as Delta tables written with [delta-rs](https://delta-io.github.io/delta-rs/)
(`deltalake`), no Spark Delta jars needed:

| Table | Partitioned by | Contents |
|---|---|---|
| `stream_output/delta/zone_window_counts` | `window_date` | `window_start`, `window_end`, `zone`, `trip_count`, `finalized_at_watermark`, `batch_id` |
| `stream_output/delta/late_events` | `event_date` | every input column + `late_reason`, `window_start`, `window_end`, `watermark`, `batch_id` |

* **Exactly-once per micro-batch:** each append commits a Delta `txn` action
  (`appId` = Spark query id from the checkpoint, `version` = batch id) atomically
  with the data. A replayed batch finds its version already committed and is
  skipped; a new checkpoint gets a new query id, so it is never mistaken for the old one.
* All timestamps are UTC (`timestamp` with time zone); `late_events` uses the
  superset schema, so v1 rows simply have a null `surge_multiplier`.
* After the run the job OPTIMIZEs (bin-packs) both tables and writes a log
  checkpoint (`--skip-compact` to skip). No VACUUM, so time travel still works:

```python
from deltalake import DeltaTable
dt = DeltaTable("stream_output/delta/zone_window_counts")
dt.to_pyarrow_table()                       # current
DeltaTable(dt.table_uri, version=10).to_pyarrow_table()   # as of micro-batch commit 10
dt.history()                                # one WRITE per micro-batch, then OPTIMIZE
```

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
.venv\Scripts\python -m unittest -v                      # includes a ~30 s Spark test
```
