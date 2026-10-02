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
| `stream_output/delta/zone_window_counts` | `window_date` | `window_start`, `window_end`, `zone`, `trip_count`, `finalized_at_watermark`, `batch_id`, + `avg_surge_multiplier` from day 4 |
| `stream_output/delta/late_events` | `event_date` | every input column + `late_reason`, `window_start`, `window_end`, `watermark`, `batch_id` (`surge_multiplier` from day 4) |

* **Exactly-once per micro-batch:** each append commits a Delta `txn` action
  (`appId` = Spark query id from the checkpoint, `version` = batch id) atomically
  with the data. A replayed batch finds its version already committed and is
  skipped; a new checkpoint gets a new query id, so it is never mistaken for the old one.
* All timestamps are UTC (`timestamp` with time zone).
* **Schema evolution:** both tables are created with the schema-v1 columns only.
  The first micro-batch that carries a surge value adds the column
  (`schema_mode="merge"`) in the same atomic commit, tagged
  `tidewater.schemaEvolution` in the Delta history. Existing files are not
  rewritten; older rows read the new column as null.
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
.venv\Scripts\python -m unittest -v                      # includes a ~40 s Spark test
```

## Testing schema evolution and time travel (`delta_demo.py`)

This walkthrough shows the `surge_multiplier` column arriving mid-stream, then
replays last week's incident (a correction job overwrote a day of zone aggregates
with bad late data) and uses Delta time travel to prove what the table looked
like before the correction, then restores it.

### 1. Run the stream past day 4

The schema change happens on simulated day 4, so run at least 4 days. This
smaller run takes about 2 minutes (the default `--trips-per-day 1000
--slice-minutes 15` takes about 6):

```
.venv\Scripts\python local_stream.py --reset --days 5 --trips-per-day 300 --slice-minutes 60
```

Check that the summary ends with `reconciles : True`.

### 2. Run the demo

```
.venv\Scripts\python delta_demo.py
```

It runs four steps against `stream_output/delta`. Each run adds new commits, so
it can be run repeatedly. `--day YYYY-MM-DD` picks the day to corrupt (default:
the day with the most late trips). `--no-restore` leaves the bad version in
place so you can inspect it yourself.

**Step 1: schema evolution.** For each table, the demo prints the commit that
added the new column, the columns one version before and at that version (read
with time travel), and the null/value split per partition:

```
zone_window_counts: v71 at 2026-10-02 14:47:50 UTC - added avg_surge_multiplier at batch 72
  columns at v70: window_start, window_end, zone, trip_count, finalized_at_watermark, batch_id, window_date
  columns at v71: ..., window_date, avg_surge_multiplier
  avg_surge_multiplier by window_date (current table):
    2026-09-30   null   233   value     0
    2026-10-01   null     0   value   233
```

What to check:
* Batch 72 is the first hour of day 4 (`2026-10-01`), so the column arrived with the data.
* Days 1-3 are all null and day 4 onward all have values. The old files were not rewritten.
* `late_events` gains `surge_multiplier` later, at the first *late* v2 event.
  Day-3 events that arrive after that point still have a null `surge_multiplier`.

**Step 2: bad correction.** The demo reproduces the incident. It recounts one day
from `late_events` only and overwrites that day's partition
(`mode="overwrite", predicate="window_date = '...'"`), tagged
`tidewater.job=late-data-correction` in the commit info.

**Step 3: time travel.** The demo finds the bad commit in the history, then reads
the version just before it. It reads that version twice, once by version number
and once by commit timestamp (`load_as_version(datetime)`), and both reads must
resolve to the same version. It then compares that snapshot with the current table:

```
bad correction committed as v120; last good snapshot is v119 (by timestamp -> v119)
  zone                     v119  current   lost
  Z01-DOWNTOWN               33        0     33
  ...
  TOTAL                     291        9    282
```

This shows that before the correction the day held 291 trips, and the correction
replaced them with only the 9 late trips.

**Step 4: restore.** The demo runs `DeltaTable.restore(v119)`. The restore is a
new commit, so the history keeps the bad version for auditing. It then checks
that the day matches the snapshot again (`day matches v119: True`).

### 3. Explore by hand

```python
from deltalake import DeltaTable
path = "stream_output/delta/zone_window_counts"
dt = DeltaTable(path)

# every commit, newest first; look for tidewater.schemaEvolution / tidewater.job keys
for h in dt.history()[:5]:
    print(h["version"], h["operation"], h.get("operationParameters", {}).get("predicate"),
          h.get("tidewater.schemaEvolution") or h.get("tidewater.job", ""))

# schema before and after evolution
[f.name for f in DeltaTable(path, version=70).schema().fields]
[f.name for f in DeltaTable(path, version=71).schema().fields]

# the table as of a version, or as of a point in time
DeltaTable(path, version=119).to_pyarrow_table()
old = DeltaTable(path); old.load_as_version("2026-10-02T14:48:00Z"); old.version()
```

### 4. Automated tests

```
.venv\Scripts\python -m unittest -v test_delta_sink test_delta_demo   # no Spark, ~2 s
.venv\Scripts\python -m unittest -v                                   # everything, ~40 s
```

* `test_delta_sink`: tables start without the v2 columns, an all-null column
  doesn't evolve the schema, and the first value evolves it exactly once (still
  exactly-once on replay).
* `test_delta_demo`: bad correction, then time travel by version and timestamp,
  then restore, on a small synthetic table. Also runs the CLI end to end.
* `test_local_stream`: a 4-day Spark run whose `zone_window_counts` evolves once,
  with null `avg_surge_multiplier` before day 4 and values from day 4.

Version numbers and timestamps in the sample output vary with run size and time.
