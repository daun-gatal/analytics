#!/usr/bin/env python3
"""Streaming generator for the analytics datalake (RustFS).

Consumes two live internet firehoses and appends to Iceberg tables:

  https://stream.wikimedia.org/v2/stream/recentchange (SSE)
    -> datalake.events.wikipedia_edits
  wss://jetstream2.us-east.bsky.network/now           (WebSocket)
    -> datalake.events.bluesky_posts

Runtime facts come from env only — same philosophy as the duckdb/maintenance
modules:
  duckdb-settings ConfigMap   -> RUSTFS_PROTOCOL/_ENDPOINT_HOST/_ENDPOINT_PORT/_REGION
  rustfs-credentials Secret   -> RUSTFS_ACCESS_KEY / RUSTFS_SECRET_KEY
  generator-settings ConfigMap -> GENERATOR_* / DRY_RUN_DIR tunables

GENERATOR_DRY_RUN=true skips Iceberg entirely and writes plain Parquet files
to DRY_RUN_DIR — full pipeline proof with no RustFS/catalog integration.
"""

import json
import os
import signal
import sys
import threading
import time
import uuid
from datetime import datetime, timezone

import pyarrow as pa
import requests

# ---------------------------------------------------------------------------
# Tunables (env)
# ---------------------------------------------------------------------------

CATALOG_NAME = os.getenv("GENERATOR_CATALOG_NAME", "datalake")
DRY_RUN = os.getenv("GENERATOR_DRY_RUN", "false").lower() in ("1", "true", "yes")
DRY_RUN_DIR = os.getenv("DRY_RUN_DIR", "/tmp/dryrun")

FLUSH_MAX_ROWS = int(os.getenv("GENERATOR_FLUSH_MAX_ROWS", "100"))
FLUSH_INTERVAL_S = int(os.getenv("GENERATOR_FLUSH_INTERVAL_S", "60"))
# Test/dev levers: stop after N events per source / M seconds. 0 = run forever.
MAX_EVENTS_PER_SOURCE = int(os.getenv("GENERATOR_MAX_EVENTS", "0"))
DURATION_S = int(os.getenv("GENERATOR_DURATION_S", "0"))

WIKIMEDIA_URL = os.getenv(
    "GENERATOR_WIKIMEDIA_URL",
    "https://stream.wikimedia.org/v2/stream/recentchange",
)
JETSTREAM_URL = os.getenv(
    "GENERATOR_JETSTREAM_URL",
    "wss://jetstream.us-east.bsky.network/subscribe",
)
USER_AGENT = "analytics-generator/1.0 (datalake demo; k8s CronJob/Deployment)"

HEARTBEAT_PATH = "/tmp/heartbeat"

TABLES = ("wikipedia_edits", "bluesky_posts")

PA_TS = pa.timestamp("us", tz="UTC")

# pyarrow mirror of the Iceberg schemas below (pyiceberg accepts these as-is).
ARROW_SCHEMAS = {
    "wikipedia_edits": pa.schema(
        [
            ("event_ts", PA_TS),
            ("wiki_db", pa.string()),
            ("event_type", pa.string()),
            ("title", pa.string()),
            ("editor", pa.string()),
            ("is_bot", pa.bool_()),
            ("revision_id", pa.int64()),
            ("comment", pa.string()),
        ]
    ),
    "bluesky_posts": pa.schema(
        [
            ("event_ts", PA_TS),
            ("did", pa.string()),
            ("post_uri", pa.string()),
            ("action", pa.string()),
            ("text", pa.string()),
            ("langs", pa.list_(pa.string())),
            ("raw", pa.string()),
        ]
    ),
}

# ---------------------------------------------------------------------------
# Iceberg side (imported lazily so DRY_RUN needs only pyarrow + requests)
# ---------------------------------------------------------------------------

_iceberg = None  # (catalog, {table_name: Table})


def fail(message, missing=None):
    """Fail fast with a clear message — mirror generate-config.sh style."""
    if missing:
        message = f"{message}: {', '.join(missing)}"
    print(f"❌ streaming: {message}", file=sys.stderr, flush=True)
    sys.exit(2)


def load_iceberg_catalog():
    """REST catalog on RustFS with SigV4, derived purely from env facts."""
    protocol = os.getenv("RUSTFS_PROTOCOL", "http")
    host = os.getenv("RUSTFS_ENDPOINT_HOST")
    port = os.getenv("RUSTFS_ENDPOINT_PORT", "9000")
    region = os.getenv("RUSTFS_REGION", "us-east-1")
    access_key = os.getenv("RUSTFS_ACCESS_KEY")
    secret_key = os.getenv("RUSTFS_SECRET_KEY")
    if not host:
        fail("RUSTFS_ENDPOINT_HOST is not set (envFrom duckdb-settings)")
    if not access_key or not secret_key:
        fail("RUSTFS credentials not set (envFrom rustfs-credentials)")

    from pyiceberg.catalog import load_catalog
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import DayTransform
    from pyiceberg.types import (
        BooleanType,
        ListType,
        LongType,
        NestedField,
        StringType,
        TimestampType,
    )

    base = f"{protocol}://{host}:{port}"
    catalog = load_catalog(
        CATALOG_NAME,
        **{
            "type": "rest",
            "uri": f"{base}/iceberg",
            "warehouse": CATALOG_NAME,
            "s3.endpoint": base,
            "s3.access-key-id": access_key,
            "s3.secret-access-key": secret_key,
            "s3.region": region,
            "s3.path-style-access": "true",
            "rest.sigv4-enabled": "true",
            "rest.signing-region": region,
            "rest.signing-name": "s3",
        },
    )

    def partition_of(ts_field_id):
        return PartitionSpec(
            PartitionField(source_id=ts_field_id, field_id=1000,
                           transform=DayTransform(), name="day")
        )

    schemas = {
        "wikipedia_edits": Schema(
            NestedField(1, "event_ts", TimestampType(), required=True),
            NestedField(2, "wiki_db", StringType(), required=True),
            NestedField(3, "event_type", StringType(), required=True),
            NestedField(4, "title", StringType(), required=True),
            NestedField(5, "editor", StringType(), required=True),
            NestedField(6, "is_bot", BooleanType(), required=False),
            NestedField(7, "revision_id", LongType(), required=False),
            NestedField(8, "comment", StringType(), required=False),
        ),
        "bluesky_posts": Schema(
            NestedField(1, "event_ts", TimestampType(), required=True),
            NestedField(2, "did", StringType(), required=True),
            NestedField(3, "post_uri", StringType(), required=True),
            NestedField(4, "action", StringType(), required=True),
            NestedField(5, "text", StringType(), required=False),
            NestedField(6, "langs", ListType(element_id=7,
                                             element_type=StringType()),
                        required=False),
            NestedField(8, "raw", StringType(), required=False),
        ),
    }
    tables = {
        name: catalog.create_table_if_not_exists(
            f"events.{name}", schema=schemas[name],
            partition_spec=partition_of(1),
        )
        for name in TABLES
    }
    return tables


def write_iceberg(table_name, rows):
    """Append one buffered batch as a single Iceberg snapshot."""
    global _iceberg
    if _iceberg is None:
        _iceberg = load_iceberg_catalog()
    arrow = pa.Table.from_pylist(rows, schema=ARROW_SCHEMAS[table_name])
    with WRITE_LOCK:
        _iceberg[table_name].append(arrow)
    print(f"stream: committed {len(rows)} rows -> events.{table_name}",
          flush=True)


def write_dry_run(table_name, rows):
    """Dry-run: plain Parquet files on local disk, no catalog at all."""
    os.makedirs(DRY_RUN_DIR, exist_ok=True)
    arrow = pa.Table.from_pylist(rows, schema=ARROW_SCHEMAS[table_name])
    seq = DRY_RUN_SEQ[table_name]
    path = os.path.join(DRY_RUN_DIR, f"{table_name}-{seq:04d}.parquet")
    DRY_RUN_SEQ[table_name] += 1
    import pyarrow.parquet as pq

    pq.write_table(arrow, path)
    print(f"stream: DRY-RUN {len(rows)} rows -> {path}", flush=True)


DRY_RUN_SEQ = {name: 0 for name in TABLES}
WRITE_LOCK = threading.Lock()
WRITE = write_dry_run if DRY_RUN else write_iceberg

# ---------------------------------------------------------------------------
# Buffering: small in-memory batches -> few, bigger Iceberg commits
# ---------------------------------------------------------------------------


class TableBuffer:
    def __init__(self, name):
        self.name = name
        self.rows = []
        self.last_flush = time.monotonic()
        self.lock = threading.Lock()

    def add(self, row):
        overflow = False
        with self.lock:
            self.rows.append(row)
            overflow = len(self.rows) >= FLUSH_MAX_ROWS
        if overflow:
            self.flush()

    def flush(self, force=False):
        idle = time.monotonic() - self.last_flush >= FLUSH_INTERVAL_S
        if not (force or idle):
            return
        with self.lock:
            rows, self.rows = self.rows, []
        self.last_flush = time.monotonic()
        if rows:
            try:
                WRITE(self.name, rows)
            except Exception as exc:  # keep the pod alive; drop this batch
                print(f"stream: flush failed for {self.name}: {exc}",
                      file=sys.stderr, flush=True)


BUFFERS = {name: TableBuffer(name) for name in TABLES}
COUNTS = {name: 0 for name in TABLES}
COUNT_LOCK = threading.Lock()
STOP = threading.Event()


def bump(name):
    with COUNT_LOCK:
        COUNTS[name] += 1
        total = COUNTS[name]
    if MAX_EVENTS_PER_SOURCE and total >= MAX_EVENTS_PER_SOURCE:
        STOP.set()


def flush_all(force=False):
    for buffer in BUFFERS.values():
        buffer.flush(force=force)


# ---------------------------------------------------------------------------
# Source 1: Wikimedia EventStreams (SSE)
# ---------------------------------------------------------------------------


def parse_wikimedia(payload):
    """recentchange SSE event -> wikipedia_edits row (or None to skip)."""
    try:
        event = json.loads(payload)
    except json.JSONDecodeError:
        return None
    if event.get("type") not in ("edit", "new"):
        return None
    meta = event.get("meta") or {}
    dt = meta.get("dt")
    if not dt:
        return None
    revision = event.get("revision") or {}
    return {
        "event_ts": datetime.fromisoformat(dt.replace("Z", "+00:00")),
        "wiki_db": event.get("server_name") or "unknown",
        "event_type": event["type"],
        "title": event.get("title") or "",
        "editor": event.get("user") or "",
        "is_bot": bool(event.get("bot")),
        "revision_id": revision.get("new"),
        "comment": event.get("comment"),
    }


def wikimedia_worker():
    backoff = 1
    while not STOP.is_set():
        try:
            with requests.get(
                WIKIMEDIA_URL,
                stream=True,
                timeout=(10, 120),
                headers={"User-Agent": USER_AGENT, "Accept": "text/event-stream"},
            ) as response:
                response.raise_for_status()
                backoff = 1
                for line in response.iter_lines(decode_unicode=True):
                    if STOP.is_set():
                        return
                    if not line or not line.startswith("data:"):
                        continue
                    row = parse_wikimedia(line[5:].strip())
                    if row:
                        BUFFERS["wikipedia_edits"].add(row)
                        bump("wikipedia_edits")
        except Exception as exc:
            if STOP.is_set():
                return
            print(f"stream: wikimedia disconnected ({exc}); retry in {backoff}s",
                  file=sys.stderr, flush=True)
            STOP.wait(backoff)
            backoff = min(backoff * 2, 60)


# ---------------------------------------------------------------------------
# Source 2: Bluesky Jetstream (WebSocket)
# ---------------------------------------------------------------------------


def parse_jetstream(message):
    """Jetstream frame -> bluesky_posts row (or None to skip)."""
    try:
        event = json.loads(message)
    except json.JSONDecodeError:
        return None
    if event.get("kind") != "commit":
        return None
    commit = event.get("commit") or {}
    if commit.get("collection") != "app.bsky.feed.post":
        return None
    operation = commit.get("operation")
    if operation not in ("create", "delete"):
        return None
    did = event.get("did") or ""
    rkey = commit.get("rkey") or ""
    time_us = event.get("time_us")
    if not did or not rkey or not time_us:
        return None
    record = commit.get("record") or {}
    return {
        "event_ts": datetime.fromtimestamp(time_us / 1_000_000, tz=timezone.utc),
        "did": did,
        "post_uri": f"at://{did}/app.bsky.feed.post/{rkey}",
        "action": operation,
        "text": record.get("text"),
        "langs": record.get("langs"),
        "raw": json.dumps(event, separators=(",", ":")),
    }


def jetstream_worker():
    import asyncio

    import websockets

    async def loop():
        backoff = 1
        while not STOP.is_set():
            try:
                async with websockets.connect(JETSTREAM_URL) as ws:
                    backoff = 1
                    async for message in ws:
                        if STOP.is_set():
                            return
                        row = parse_jetstream(message)
                        if row:
                            BUFFERS["bluesky_posts"].add(row)
                            bump("bluesky_posts")
            except Exception as exc:
                if STOP.is_set():
                    return
                print(f"stream: jetstream disconnected ({exc}); retry in {backoff}s",
                      file=sys.stderr, flush=True)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    asyncio.run(loop())


# ---------------------------------------------------------------------------
# Main: supervise both sources, flush on timer, heartbeat for the probe
# ---------------------------------------------------------------------------


def main():
    if DRY_RUN:
        print(f"stream: DRY-RUN mode — Parquet to {DRY_RUN_DIR}, no Iceberg",
              flush=True)

    started = time.monotonic()
    threading.Thread(target=wikimedia_worker, daemon=True, name="wikimedia").start()
    threading.Thread(target=jetstream_worker, daemon=True, name="jetstream").start()

    def shutdown(signum, _frame):
        print(f"stream: signal {signum} — flushing and stopping", flush=True)
        STOP.set()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    while not STOP.is_set():
        time.sleep(5)
        # liveness heartbeat for the kubelet exec probe
        try:
            with open(HEARTBEAT_PATH, "w") as fh:
                fh.write(str(time.time()))
        except OSError:
            pass
        flush_all()
        if DURATION_S and time.monotonic() - started >= DURATION_S:
            STOP.set()

    flush_all(force=True)
    with COUNT_LOCK:
        stats = dict(COUNTS)
    print(f"stream: done — {stats}", flush=True)
    # Dry-run proof: show what landed on disk.
    if DRY_RUN and os.path.isdir(DRY_RUN_DIR):
        for name in sorted(os.listdir(DRY_RUN_DIR)):
            print(f"stream: DRY-RUN output {DRY_RUN_DIR}/{name}", flush=True)


if __name__ == "__main__":
    main()
