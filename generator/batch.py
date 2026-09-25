#!/usr/bin/env python3
"""Batch generator for the analytics datalake (RustFS).

Ingests hourly public dump files from the internet into Iceberg tables:

  https://data.gharchive.org/{YYYY-MM-DD-H}.json.gz   (every public GitHub event)
    -> datalake.web.github_events
  https://dumps.wikimedia.org/other/pageviews/pageviews-{YYYYMMDD}-{HH}0000.gz
    -> datalake.web.wikimedia_pageviews

Both sources are ingested with overwrite-by-hour semantics: re-running the
CronJob for the same hour REPLACES the partition slice instead of duplicating
rows. Wikimedia pageview dumps publish ~2-4h behind, so the job walks back
from the previous complete hour until it finds a published file.

Runtime facts come from env only — same philosophy as the duckdb/maintenance
modules:
  duckdb-settings ConfigMap    -> RUSTFS_PROTOCOL/_ENDPOINT_HOST/_ENDPOINT_PORT/_REGION
  rustfs-credentials Secret    -> RUSTFS_ACCESS_KEY / RUSTFS_SECRET_KEY
  generator-settings ConfigMap -> GENERATOR_* / DRY_RUN_DIR tunables

GENERATOR_DRY_RUN=true skips Iceberg entirely and writes plain Parquet files
to DRY_RUN_DIR — full pipeline proof with no RustFS/catalog integration.
"""

import gzip
import io
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import requests

# ---------------------------------------------------------------------------
# Tunables (env)
# ---------------------------------------------------------------------------

CATALOG_NAME = os.getenv("GENERATOR_CATALOG_NAME", "datalake")
DRY_RUN = os.getenv("GENERATOR_DRY_RUN", "false").lower() in ("1", "true", "yes")
DRY_RUN_DIR = os.getenv("DRY_RUN_DIR", "/tmp/dryrun")

# Safety valve: 0 = unlimited rows per source per run; otherwise truncate.
BATCH_MAX_ROWS = int(os.getenv("GENERATOR_BATCH_MAX_ROWS", "0"))
# How far back to hunt for the newest published pageview dump (~2-4h lag).
BACKTRACK_HOURS = int(os.getenv("GENERATOR_BATCH_BACKTRACK_HOURS", "6"))

GHARCHIVE_URL = "https://data.gharchive.org/{stamp}.json.gz"
PAGEVIEWS_URL = ("https://dumps.wikimedia.org/other/pageviews/{year}/{month}"
                 "/pageviews-{stamp}.gz")
USER_AGENT = "analytics-generator/1.0 (datalake demo; k8s CronJob/Deployment)"

PA_TS = pa.timestamp("us", tz="UTC")

def _field(name, dtype, nullable=False):
    """pa.field with explicit nullability — pyiceberg checks that the Arrow
    schema matches the Iceberg schema field-by-field, including required."""
    return pa.field(name, dtype, nullable=nullable)


S = pa.string()


def _schema(*fields):
    return pa.schema(fields)


ARROW_SCHEMAS = {
    "github_events": _schema(
        _field("event_ts", PA_TS),
        _field("event_type", S),
        pa.field("actor_login", S),
        pa.field("repo_name", S),
        pa.field("action", S),
        pa.field("push_size", pa.int64()),
    ),
    "wikimedia_pageviews": _schema(
        _field("view_ts", PA_TS),
        _field("project", S),
        _field("page_title", S),
        _field("views", pa.int64()),
    ),
}

# ---------------------------------------------------------------------------
# Iceberg side (imported lazily so DRY_RUN needs only pyarrow + requests)
# ---------------------------------------------------------------------------


def fail(message, missing=None):
    """Fail fast with a clear message — mirror generate-config.sh style."""
    if missing:
        message = f"{message}: {', '.join(missing)}"
    print(f"❌ batch: {message}", file=sys.stderr, flush=True)
    sys.exit(2)


def _clean_credential(name, raw):
    """Strip whitespace and hard-fail on unusable credential values.

    GitHub Secrets often carry a trailing newline; any control character in
    the SigV4-signed Authorization header makes the HTTP client reject the
    request with 'invalid header: authorization'. Stripping transparently
    fixes the common case; anything still containing whitespace fails fast
    with an actionable message.
    """
    value = (raw or "").strip()
    if value != (raw or ""):
        print(f"⚠ batch: {name} had surrounding whitespace/newline"
              " — stripped (check the GitHub Secret)", file=sys.stderr,
              flush=True)
    if not value or any(c.isspace() for c in value):
        fail(f"{name} is empty or contains whitespace characters"
             " (check the GitHub Secret for stray spaces/newlines)")
    return value


def catalog_properties():
    """Catalog connection, derived purely from env facts.

    Defaults build the RustFS REST catalog from duckdb-settings facts
    (RUSTFS_PROTOCOL/_ENDPOINT_HOST/_ENDPOINT_PORT/_REGION) + rustfs-credentials.
    GENERATOR_CATALOG_TYPE/URI/WAREHOUSE override everything for testing
    against any other catalog (e.g. a local sqlite catalog with a file
    warehouse — the full Iceberg write path with no RustFS at all).
    """
    catalog_type = os.getenv("GENERATOR_CATALOG_TYPE", "rest")
    props = {
        "type": catalog_type,
        "warehouse": os.getenv("GENERATOR_CATALOG_WAREHOUSE", CATALOG_NAME),
    }
    uri = os.getenv("GENERATOR_CATALOG_URI")
    if not uri:
        # derive the REST endpoint from the shared duckdb-settings facts
        protocol = os.getenv("RUSTFS_PROTOCOL", "http")
        host = os.getenv("RUSTFS_ENDPOINT_HOST")
        port = os.getenv("RUSTFS_ENDPOINT_PORT", "9000")
        if not host:
            fail("RUSTFS_ENDPOINT_HOST is not set (envFrom duckdb-settings)")
        uri = f"{protocol}://{host}:{port}/iceberg"
    props["uri"] = uri

    if catalog_type == "rest":
        region = os.getenv("RUSTFS_REGION", "us-east-1")
        access_key = _clean_credential("RUSTFS_ACCESS_KEY",
                                       os.getenv("RUSTFS_ACCESS_KEY"))
        secret_key = _clean_credential("RUSTFS_SECRET_KEY",
                                       os.getenv("RUSTFS_SECRET_KEY"))
        base = uri.rsplit("/iceberg", 1)[0]
        props.update({
            "s3.endpoint": base,
            # s3.* feeds the S3 FileIO data plane (Parquet reads/writes)
            "s3.access-key-id": access_key,
            "s3.secret-access-key": secret_key,
            "s3.region": region,
            "s3.path-style-access": "true",
            # client.* feeds the boto3 session that SIGV4-signs the REST
            # catalog requests — a SEPARATE set of properties from s3.*!
            # Missing client.* makes botocore sign with fallback credentials
            # and RustFS rejects the resulting header with
            # 400 "invalid header: authorization".
            "client.access-key-id": access_key,
            "client.secret-access-key": secret_key,
            "client.region": region,
            "rest.sigv4-enabled": "true",
            "rest.signing-region": region,
            "rest.signing-name": "s3",
        })
    return props


def ensure_namespace(catalog, identifier):
    """Create the namespace (db/schema) if missing — e.g. web / events.

    create_namespace_if_not_exists is idempotent, so every run can call it
    safely before create_table_if_not_exists.
    """
    namespace = identifier.split(".")[0]
    catalog.create_namespace_if_not_exists(namespace)


def load_table(table_name):
    """Open (or create) the web.<table> Iceberg table."""

    from pyiceberg.catalog import load_catalog
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import DayTransform
    from pyiceberg.types import (
        LongType,
        NestedField,
        StringType,
        TimestamptzType,
    )

    schemas = {
        "github_events": Schema(
            NestedField(1, "event_ts", TimestamptzType(), required=True),
            NestedField(2, "event_type", StringType(), required=True),
            NestedField(3, "actor_login", StringType(), required=False),
            NestedField(4, "repo_name", StringType(), required=False),
            NestedField(5, "action", StringType(), required=False),
            NestedField(6, "push_size", LongType(), required=False),
        ),
        "wikimedia_pageviews": Schema(
            NestedField(1, "view_ts", TimestamptzType(), required=True),
            NestedField(2, "project", StringType(), required=True),
            NestedField(3, "page_title", StringType(), required=True),
            NestedField(4, "views", LongType(), required=True),
        ),
    }
    catalog = load_catalog(CATALOG_NAME, **catalog_properties())
    identifier = f"web.{table_name}"
    ensure_namespace(catalog, identifier)
    return catalog.create_table_if_not_exists(
        identifier,
        schema=schemas[table_name],
        partition_spec=PartitionSpec(
            PartitionField(source_id=1, field_id=1000,
                           transform=DayTransform(), name="day")
        ),
    )


def overwrite_hour(table, column, hour_start, arrow):
    """Replace the [hour_start, hour_start+1h) slice — idempotent re-runs."""
    from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan

    hour_end = hour_start + timedelta(hours=1)
    predicate = And(
        GreaterThanOrEqual(column, hour_start),
        LessThan(column, hour_end),
    )
    table.overwrite(arrow, overwrite_filter=predicate)


def write_dry_run(table_name, hour_start, arrow):
    """Dry-run: one Parquet file per table, replaced on each run."""
    os.makedirs(DRY_RUN_DIR, exist_ok=True)
    path = os.path.join(DRY_RUN_DIR, f"{table_name}.parquet")
    import pyarrow.parquet as pq

    pq.write_table(arrow, path)
    print(f"batch: DRY-RUN {arrow.num_rows} rows -> {path}", flush=True)


def write(table_name, hour_start, rows, column):
    """Rows -> Arrow -> Iceberg overwrite-by-hour (or dry-run Parquet)."""
    arrow = pa.Table.from_pylist(rows, schema=ARROW_SCHEMAS[table_name])
    if not arrow.num_rows:
        print(f"batch: {table_name} produced 0 rows for {hour_start:%Y-%m-%d %H}h"
              " — nothing to write", flush=True)
        return
    if DRY_RUN:
        write_dry_run(table_name, hour_start, arrow)
        return
    overwrite_hour(load_table(table_name), column, hour_start, arrow)
    print(f"batch: overwrote {arrow.num_rows} rows in web.{table_name}"
          f" for {hour_start:%Y-%m-%d %H}h", flush=True)


# ---------------------------------------------------------------------------
# Source 1: GH Archive — one JSONL-in-gzip file per hour
# ---------------------------------------------------------------------------


def gharchive_rows(hour_start, max_rows):
    """Stream-parse data.gharchive.org, yielding github_events rows."""
    stamp = hour_start.strftime("%Y-%m-%d-%-H")
    url = GHARCHIVE_URL.format(stamp=stamp)
    print(f"batch: fetching {url}", flush=True)
    rows, truncated = [], False
    with requests.get(url, stream=True, timeout=(10, 300),
                      headers={"User-Agent": USER_AGENT}) as response:
        response.raise_for_status()
        # stream-decompress: never hold the whole hour file in memory
        gz = gzip.GzipFile(fileobj=response.raw, mode="rb")
        text = io.TextIOWrapper(gz, encoding="utf-8", errors="replace")
        for line in text:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            payload = event.get("payload") or {}
            created = event.get("created_at") or ""
            try:
                event_ts = datetime.fromisoformat(created.replace("Z", "+00:00"))
            except ValueError:
                continue
            rows.append({
                "event_ts": event_ts,
                "event_type": event.get("type") or "",
                "actor_login": (event.get("actor") or {}).get("login"),
                "repo_name": (event.get("repo") or {}).get("name"),
                "action": payload.get("action"),
                "push_size": payload.get("size") if event.get("type") == "PushEvent" else None,
            })
            if max_rows and len(rows) >= max_rows:
                truncated = True
                break
    if truncated:
        print(f"batch: gharchive truncated at {max_rows} rows"
              " (GENERATOR_BATCH_MAX_ROWS)", flush=True)
    return rows


# ---------------------------------------------------------------------------
# Source 2: Wikimedia pageviews — hourly dump, published with a lag
# ---------------------------------------------------------------------------


def pageview_rows(hour_start, max_rows):
    """Walk back until a published dump is found; stream-parse to rows."""
    for offset in range(0, BACKTRACK_HOURS + 1):
        candidate = hour_start - timedelta(hours=offset)
        stamp = candidate.strftime("%Y%m%d-%H0000")
        url = PAGEVIEWS_URL.format(year=candidate.strftime("%Y"),
                                   month=candidate.strftime("%Y-%m"),
                                   stamp=stamp)
        print(f"batch: trying {url}", flush=True)
        try:
            response = requests.get(url, stream=True, timeout=(10, 300),
                                    headers={"User-Agent": USER_AGENT})
        except requests.RequestException as exc:
            print(f"batch: {url} unreachable ({exc})", flush=True)
            continue
        with response:
            if response.status_code != 200:
                print(f"batch: {url} -> HTTP {response.status_code} (not yet"
                      " published)", flush=True)
                continue
            rows, truncated = [], False
            # stream-decompress: never hold the whole dump in memory
            gz = gzip.GzipFile(fileobj=response.raw, mode="rb")
            text = io.TextIOWrapper(gz, encoding="utf-8", errors="replace")
            for line in text:
                parsed = parse_pageview_line(line)
                if parsed:
                    project, title, views = parsed
                    rows.append({
                        "view_ts": candidate.replace(minute=0, second=0,
                                                     microsecond=0),
                        "project": project,
                        "page_title": title,
                        "views": views,
                    })
                    if max_rows and len(rows) >= max_rows:
                        truncated = True
                        break
            if truncated:
                print(f"batch: pageviews truncated at {max_rows} rows"
                      " (GENERATOR_BATCH_MAX_ROWS)", flush=True)
            print(f"batch: pageviews file {candidate:%Y-%m-%d %H}h accepted"
                  f" ({len(rows)} rows)", flush=True)
            return rows, candidate
    print(f"batch: no pageviews dump published within {BACKTRACK_HOURS}h of"
          f" {hour_start:%Y-%m-%d %H}h — skipping this run", flush=True)
    return None, None


def parse_pageview_line(line):
    """Dump line -> (project, title, views). Handles both dump formats.

    Old format: 'domain project title count bytes' (5 fields)
    New format: 'domain title count bytes'         (4 fields, current)
    Known quirk: Wikifunctions rows carry a literal '""' domain.
    """
    parts = line.rstrip("\n").split(" ")
    if len(parts) == 5:
        project = f"{parts[0]}.{parts[1]}"
        title, count = parts[2], parts[3]
    elif len(parts) == 4:
        project = parts[0]
        title, count = parts[1], parts[2]
    else:
        return None
    if not count.isdigit() or not title or title == "-":
        return None
    if project in ('""', ""):
        project = "wikifunctions"
    return project, title, int(count)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    if DRY_RUN:
        print(f"batch: DRY-RUN mode — Parquet to {DRY_RUN_DIR}, no Iceberg",
              flush=True)
    # Previous complete hour: dump files cover a closed hour, so never aim
    # at the hour currently in progress.
    hour_start = (datetime.now(timezone.utc) - timedelta(hours=1)
                  ).replace(minute=0, second=0, microsecond=0)
    max_rows = BATCH_MAX_ROWS
    failures = 0

    try:
        rows = gharchive_rows(hour_start, max_rows)
        write("github_events", hour_start, rows, "event_ts")
    except Exception as exc:
        failures += 1
        print(f"batch: ❌ gharchive failed: {exc}", file=sys.stderr, flush=True)

    try:
        rows, accepted_hour = pageview_rows(hour_start, max_rows)
        if rows is not None:
            # overwrite the slice the dump actually covers, not prev-hour
            write("wikimedia_pageviews", accepted_hour, rows, "view_ts")
    except Exception as exc:
        failures += 1
        print(f"batch: ❌ pageviews failed: {exc}", file=sys.stderr, flush=True)

    if DRY_RUN and os.path.isdir(DRY_RUN_DIR):
        for name in sorted(os.listdir(DRY_RUN_DIR)):
            print(f"batch: DRY-RUN output {DRY_RUN_DIR}/{name}", flush=True)

    if failures:
        fail(f"{failures} of 2 sources failed")
    print("batch: done — all sources ingested", flush=True)


if __name__ == "__main__":
    main()
