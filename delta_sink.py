"""Delta Lake sinks for the zone-window stream, written with delta-rs (``deltalake``).

Tables (under ``<out>/delta/``):

* ``zone_window_counts`` - finalized tumbling windows, one row per (window, zone),
  partitioned by ``window_date``. Append-only: a window is written exactly once.
* ``late_events``        - side output of events beyond the watermark, with the
  watermark they missed and why, partitioned by ``event_date``.

Exactly-once per micro-batch: every append commits a Delta ``txn`` action
(appId = Spark query id, version = batch id) atomically with the data. Before
writing, the sink checks the table's recorded version for that appId and skips
batches it has already committed, so a replayed foreachBatch is a no-op. This is
the same mechanism Spark's own Delta streaming sink uses.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
from deltalake import CommitProperties, DeltaTable, Transaction, write_deltalake

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
TS = pa.timestamp("us", tz="UTC")

ZONE_WINDOW_COUNTS_SCHEMA = pa.schema([
    pa.field("window_start", TS, nullable=False),
    pa.field("window_end", TS, nullable=False),
    pa.field("zone", pa.string(), nullable=False),
    pa.field("trip_count", pa.int64(), nullable=False),
    pa.field("finalized_at_watermark", TS, nullable=False),
    pa.field("batch_id", pa.int64(), nullable=False),
    pa.field("window_date", pa.date32(), nullable=False),
])

# Mirrors local_stream.EVENT_SCHEMA (superset incl. schema-v2 surge_multiplier)
# plus the routing metadata.
LATE_EVENTS_SCHEMA = pa.schema([
    ("event_id", pa.string()), ("event_type", pa.string()), ("schema_version", pa.int32()),
    ("event_time", TS), ("processing_time", TS),
    ("trip_id", pa.string()), ("driver_id", pa.string()),
    ("pickup_zone", pa.string()), ("dropoff_zone", pa.string()),
    ("distance_km", pa.float64()), ("fare_usd", pa.float64()), ("surge_multiplier", pa.float64()),
    ("_delay_class", pa.string()), ("_delay_seconds", pa.float64()),
    ("late_reason", pa.string()),
    ("window_start", TS), ("window_end", TS), ("watermark", TS),
    ("batch_id", pa.int64()),
    ("event_date", pa.date32()),
])

TABLES = {
    "zone_window_counts": (ZONE_WINDOW_COUNTS_SCHEMA, ["window_date"],
                           "Finalized trips per pickup zone per tumbling event-time window"),
    "late_events": (LATE_EVENTS_SCHEMA, ["event_date"],
                    "Side output: trip events that arrived beyond the watermark"),
}


def micros_to_date(micros: int) -> date:
    return (EPOCH + timedelta(microseconds=micros)).date()


def checkpoint_query_id(checkpoint_dir: Path) -> str:
    """The streaming query's stable id, persisted by Spark in its checkpoint."""
    return json.loads((checkpoint_dir / "metadata").read_text())["id"]


class DeltaSink:
    def __init__(self, root: Path):
        self.root = root

    def path(self, name: str) -> str:
        return str(self.root / name)

    def ensure_tables(self) -> None:
        for name, (schema, partition_by, description) in TABLES.items():
            if not DeltaTable.is_deltatable(self.path(name)):
                DeltaTable.create(self.path(name), schema, partition_by=partition_by,
                                  name=name, description=description)

    def committed_version(self, name: str, app_id: str) -> int | None:
        return DeltaTable(self.path(name)).transaction_version(app_id)

    def append(self, name: str, rows: list[dict], app_id: str, batch_id: int) -> bool:
        """Append rows as micro-batch ``batch_id``; returns False if already committed."""
        dt = DeltaTable(self.path(name))
        done = dt.transaction_version(app_id)
        if done is not None and done >= batch_id:
            return False
        schema = TABLES[name][0]
        write_deltalake(
            dt, pa.Table.from_pylist(rows, schema=schema), mode="append",
            commit_properties=CommitProperties(
                app_transactions=[Transaction(app_id=app_id, version=batch_id)]),
        )
        return True

    def read(self, name: str, version: int | None = None) -> pa.Table:
        return DeltaTable(self.path(name), version=version).to_pyarrow_table()

    def compact(self) -> dict[str, dict]:
        """Bin-pack the many small per-batch files and write a log checkpoint.

        Old files stay referenced by earlier versions, so time travel still works
        until a VACUUM (deliberately not run here)."""
        metrics = {}
        for name in TABLES:
            dt = DeltaTable(self.path(name))
            metrics[name] = dt.optimize.compact()
            dt.create_checkpoint()
        return metrics

    def describe(self) -> dict[str, dict]:
        info = {}
        for name in TABLES:
            dt = DeltaTable(self.path(name))
            ops = [h["operation"] for h in dt.history()]
            info[name] = {
                "version": dt.version(),
                "files": len(dt.file_uris()),
                "rows": dt.to_pyarrow_table().num_rows,
                "operations": {op: ops.count(op) for op in sorted(set(ops))},
            }
        return info


def count_by(table: pa.Table, keys: list[str]) -> list[tuple]:
    """[(key..., count)] sorted by count desc - a tiny GROUP BY for reporting."""
    if table.num_rows == 0:
        return []
    grouped = table.group_by(keys).aggregate([([], "count_all")]).to_pylist()
    return sorted(((*(g[k] for k in keys), g["count_all"]) for g in grouped),
                  key=lambda r: (-r[-1], [str(v) for v in r[:-1]]))


def total(table: pa.Table, column: str) -> int:
    return pc.sum(table[column]).as_py() or 0
