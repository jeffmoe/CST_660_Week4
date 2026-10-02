"""Synthetic trip-event stream for Tidewater Mobility.

Emits trip events in *arrival* (processing_time) order, the way a stream
consumer would actually see them, while each event carries the separate
event_time at which it happened on the driver's phone.

The stream deliberately injects the problems we see in production:

* jitter / out-of-order  - small network delays shuffle neighbouring events
* late arrivals          - phones buffer events for minutes in the harbor
                           tunnels and flush them on exit
* very late arrivals     - hours-late events (device offline, retries) that
                           land well past any reasonable watermark
* schema change          - from simulated day 4 the app starts sending a new
                           ``surge_multiplier`` column (schema_version 2)

Only the standard library is used. Output is deterministic for a given seed.
"""

from __future__ import annotations

import argparse
import heapq
import itertools
import json
import random
import sys
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterator

# --------------------------------------------------------------------------
# Simulation constants
# --------------------------------------------------------------------------

# Pricing zones. Tunnel zones have phones that lose connectivity and buffer.
ZONES = {
    "Z01-DOWNTOWN": False,
    "Z02-WATERFRONT": False,
    "Z03-HARBOR-TUNNEL-N": True,
    "Z04-HARBOR-TUNNEL-S": True,
    "Z05-NAVAL-BASE": False,
    "Z06-AIRPORT": False,
    "Z07-BEACH": False,
    "Z08-UNIVERSITY": False,
}

# Relative trip demand by hour of day (rush hours peak).
HOURLY_DEMAND = [
    0.2, 0.1, 0.1, 0.1, 0.2, 0.5, 1.0, 1.8, 2.0, 1.2, 0.9, 1.0,
    1.2, 1.0, 0.9, 1.1, 1.6, 2.0, 1.8, 1.3, 1.0, 0.8, 0.6, 0.4,
]

SCHEMA_CHANGE_DAY = 4  # 1-based simulated day on which surge_multiplier appears

# (delay_class, probability, min_seconds, max_seconds)
NORMAL_DELAYS = [
    ("on_time", 0.86, 0.05, 3.0),
    ("out_of_order", 0.08, 5.0, 90.0),
    ("late", 0.05, 120.0, 900.0),
    ("very_late", 0.01, 3_600.0, 6 * 3_600.0),
]
# Inside a harbor tunnel, phones buffer far more often.
TUNNEL_DELAYS = [
    ("on_time", 0.45, 0.05, 3.0),
    ("out_of_order", 0.10, 5.0, 90.0),
    ("late", 0.42, 120.0, 900.0),
    ("very_late", 0.03, 3_600.0, 6 * 3_600.0),
]


@dataclass(order=True)
class _Queued:
    sort_key: datetime
    seq: int
    event: dict


def _iso(ts: datetime) -> str:
    return ts.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _pick_delay(rng: random.Random, in_tunnel: bool) -> tuple[str, float]:
    table = TUNNEL_DELAYS if in_tunnel else NORMAL_DELAYS
    roll = rng.random()
    cumulative = 0.0
    for name, prob, lo, hi in table:
        cumulative += prob
        if roll < cumulative:
            return name, rng.uniform(lo, hi)
    name, _, lo, hi = table[-1]
    return name, rng.uniform(lo, hi)


def _surge_for(rng: random.Random, hour: int) -> float:
    base = 1.0 + max(0.0, HOURLY_DEMAND[hour] - 1.0)  # 1.0 .. 2.0
    jitter = rng.choice([0.0, 0.0, 0.25, 0.5])
    return round(min(3.0, base + jitter) * 4) / 4  # quarter steps


def _trips_for_day(
    rng: random.Random, day_start: datetime, day_number: int, trips_per_day: int
) -> list[dict]:
    """Build the trip_started / trip_completed events for one simulated day."""
    zone_names = list(ZONES)
    events = []
    for _ in range(trips_per_day):
        hour = rng.choices(range(24), weights=HOURLY_DEMAND)[0]
        start = day_start + timedelta(hours=hour, seconds=rng.uniform(0, 3_600))
        duration = timedelta(minutes=rng.uniform(4, 45))
        end = start + duration
        trip_id = str(uuid.UUID(int=rng.getrandbits(128), version=4))
        driver_id = f"D{rng.randint(1, 400):04d}"
        pickup = rng.choice(zone_names)
        dropoff = rng.choice(zone_names)
        distance_km = round(duration.total_seconds() / 60 * rng.uniform(0.35, 0.8), 2)

        common = {
            "trip_id": trip_id,
            "driver_id": driver_id,
            "pickup_zone": pickup,
            "dropoff_zone": dropoff,
        }
        has_surge = day_number >= SCHEMA_CHANGE_DAY
        surge = _surge_for(rng, hour) if has_surge else None
        fare = round((2.50 + 1.35 * distance_km) * (surge or 1.0), 2)

        for event_type, ts, zone in (
            ("trip_started", start, pickup),
            ("trip_completed", end, dropoff),
        ):
            event = {
                "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
                "event_type": event_type,
                "schema_version": 2 if has_surge else 1,
                "_event_dt": ts,
                "_zone": zone,
                **common,
            }
            if event_type == "trip_completed":
                event["distance_km"] = distance_km
                event["fare_usd"] = fare
            if has_surge:
                event["surge_multiplier"] = surge
            events.append(event)
    return events


def generate_trip_events(
    num_days: int = 7,
    trips_per_day: int = 2_000,
    start_date: datetime | None = None,
    seed: int | None = 42,
    annotate: bool = False,
) -> Iterator[dict]:
    """Yield synthetic trip events in processing_time (arrival) order.

    Each event has ISO-8601 UTC ``event_time`` (when it happened on the phone)
    and ``processing_time`` (when the pipeline received it). Because arrivals
    are delayed by varying amounts, event_time is *not* monotonic in the
    output: that is the point.

    Schema v1 (days 1-3)::

        event_id, event_type, schema_version, event_time, processing_time,
        trip_id, driver_id, pickup_zone, dropoff_zone,
        [distance_km, fare_usd]          # trip_completed only

    Schema v2 (trips starting on day 4 onward) adds ``surge_multiplier``.
    The version is fixed when the trip is created on the phone, so buffered
    day-3 events (and day-3 trips finishing after midnight) arrive on day 4
    as v1 and the two schemas interleave in the stream around the boundary.

    With ``annotate=True`` each event also carries ground-truth
    ``_delay_class`` and ``_delay_seconds`` fields for testing; a real
    consumer would never see these.

    Memory is bounded: one day of trips plus the in-flight arrival buffer.
    """
    rng = random.Random(seed)
    if start_date is None:
        start_date = datetime(2026, 9, 28, tzinfo=timezone.utc)
    elif start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=timezone.utc)
    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)

    seq = itertools.count()
    by_event_time: list[_Queued] = []  # generated, not yet "sent"
    in_flight: list[_Queued] = []  # sent, keyed by arrival time

    def send(event: dict) -> Iterator[dict]:
        """Move one event onto the wire; yield everything that has arrived by now."""
        now = event["_event_dt"]
        in_tunnel = ZONES[event["_zone"]]
        delay_class, delay_s = _pick_delay(rng, in_tunnel)
        arrival = now + timedelta(seconds=delay_s)
        event["_delay_class"] = delay_class
        event["_delay_seconds"] = round(delay_s, 3)
        event["_arrival_dt"] = arrival
        heapq.heappush(in_flight, _Queued(arrival, next(seq), event))
        while in_flight and in_flight[0].sort_key <= now:
            yield _finalize(heapq.heappop(in_flight).event, annotate)

    for day_index in range(num_days):
        day_start = start_date + timedelta(days=day_index)
        for ev in _trips_for_day(rng, day_start, day_index + 1, trips_per_day):
            heapq.heappush(by_event_time, _Queued(ev["_event_dt"], next(seq), ev))
        # Release everything that happened before the next day starts; trips
        # straddling midnight stay queued and merge with tomorrow's events.
        next_day = day_start + timedelta(days=1)
        last_day = day_index == num_days - 1
        while by_event_time and (last_day or by_event_time[0].sort_key < next_day):
            yield from send(heapq.heappop(by_event_time).event)

    while in_flight:
        yield _finalize(heapq.heappop(in_flight).event, annotate)


def _finalize(event: dict, annotate: bool) -> dict:
    event_dt = event.pop("_event_dt")
    arrival_dt = event.pop("_arrival_dt")
    event.pop("_zone")
    delay_class = event.pop("_delay_class")
    delay_s = event.pop("_delay_seconds")
    out = {
        "event_id": event.pop("event_id"),
        "event_type": event.pop("event_type"),
        "schema_version": event.pop("schema_version"),
        "event_time": _iso(event_dt),
        "processing_time": _iso(arrival_dt),
        **event,
    }
    if annotate:
        out["_delay_class"] = delay_class
        out["_delay_seconds"] = delay_s
    return out


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def _summarize(events: list[dict]) -> str:
    classes = Counter(e.get("_delay_class", "?") for e in events)
    versions = Counter(e["schema_version"] for e in events)
    out_of_order = 0
    max_event_time = ""
    for e in events:
        if e["event_time"] < max_event_time:
            out_of_order += 1
        else:
            max_event_time = e["event_time"]
    lines = [
        f"events emitted        : {len(events):,}",
        f"arrived out of order  : {out_of_order:,} ({out_of_order / max(len(events), 1):.1%})",
        f"schema versions       : {dict(sorted(versions.items()))}",
    ]
    if "?" not in classes:
        lines.append(f"delay classes         : {dict(classes.most_common())}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--trips-per-day", type=int, default=2_000)
    parser.add_argument("--start-date", type=datetime.fromisoformat, default=None,
                        help="first simulated day, e.g. 2026-09-28 (UTC)")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--annotate", action="store_true",
                        help="include ground-truth _delay_class/_delay_seconds")
    parser.add_argument("--out", default="-", help="JSONL output path, '-' for stdout")
    parser.add_argument("--summary", action="store_true",
                        help="print stream statistics to stderr")
    args = parser.parse_args(argv)

    stream = generate_trip_events(
        num_days=args.days,
        trips_per_day=args.trips_per_day,
        start_date=args.start_date,
        seed=args.seed,
        annotate=args.annotate or args.summary,
    )
    sink = sys.stdout if args.out == "-" else open(args.out, "w", encoding="utf-8")
    seen = []
    try:
        for event in stream:
            if args.summary:
                seen.append(event)
                if not args.annotate:
                    event = {k: v for k, v in event.items() if not k.startswith("_")}
            sink.write(json.dumps(event) + "\n")
    finally:
        if sink is not sys.stdout:
            sink.close()
    if args.summary:
        print(_summarize(seen), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
