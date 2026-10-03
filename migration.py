"""
migration.py - One-shot raw copy of InfluxDB -> TimescaleDB.

No preprocessing: every point in the bucket is copied as-is.
For each day-sized window it:
    1. queries InfluxDB (pivoted: one row per timestamp, fields + tags as keys)
    2. deletes any rows already in TimescaleDB for that window
    3. inserts the window, in a single transaction
Finally it compares row counts per window range.

Table layout (schema-agnostic, so nothing is dropped):
    sensor_data(time TIMESTAMPTZ, measurement TEXT, data JSONB)
    "data" holds every field and tag from Influx as JSON keys.

Safe to re-run: each window is replaced, never duplicated.

Usage:
    python migration.py                       # last 30 days
    python migration.py --days 90
    python migration.py --start 2026-08-01 --end 2026-09-01
    python migration.py --retention-days 60   # keep 60 days instead of 30 (0 = forever)
    python migration.py --dry-run             # read from Influx only

Environment (a .env next to this file is loaded automatically):
    INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET
    INFLUX_VERIFY_SSL   (default: false)
    PG_HOST (localhost)  PG_PORT (5432)  PG_DATABASE (intellisenz)
    PG_USER (postgres)   PG_PASSWORD (required)
    PG_SSLMODE (prefer; use "require" for most cloud providers)
"""

import argparse
import logging
import math
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import certifi
import psycopg2
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient
from psycopg2 import extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s  %(message)s")
logger = logging.getLogger("migration")

WINDOW = timedelta(days=1)


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing environment variable: {name}")
    return value


def _clean(value):
    """JSONB rejects NaN/Infinity; datetimes need to be strings."""
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _parse_date(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ------------------------------- InfluxDB ---------------------------------- #
def make_influx_client() -> InfluxDBClient:
    verify = os.getenv("INFLUX_VERIFY_SSL", "false").strip().lower() in {
        "1", "true", "yes", "on",
    }
    return InfluxDBClient(
        url=_require("INFLUX_URL"),
        token=_require("INFLUX_TOKEN"),
        org=_require("INFLUX_ORG"),
        verify_ssl=verify,
        # Use certifi's CA bundle so verification works even when the system
        # Python (e.g. python.org builds on macOS) has no root certs installed.
        ssl_ca_cert=certifi.where() if verify else None,
        timeout=120_000,  # ms
    )


def fetch_window(client, start: datetime, stop: datetime) -> list:
    bucket = _require("INFLUX_BUCKET")
    flux = f"""
        from(bucket: "{bucket}")
            |> range(start: {_iso(start)}, stop: {_iso(stop)})
            |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
    """
    records = []
    for table in client.query_api().query(query=flux, org=_require("INFLUX_ORG")):
        for rec in table.records:
            data = {
                k: _clean(v)
                for k, v in rec.values.items()
                if not k.startswith("_") and k not in ("result", "table")
            }
            records.append((rec.get_time(), rec.get_measurement() or "", data))
    return records


# ------------------------------ TimescaleDB -------------------------------- #
def connect_pg():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DATABASE", "intellisenz"),
        user=os.getenv("PG_USER", "postgres"),
        password=_require("PG_PASSWORD"),
        sslmode=os.getenv("PG_SSLMODE", "prefer"),
    )


def create_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sensor_data (
                time        TIMESTAMPTZ NOT NULL,
                measurement TEXT        NOT NULL,
                data        JSONB
            );
            """
        )
        cur.execute(
            """
            SELECT create_hypertable('sensor_data', 'time',
                chunk_time_interval => INTERVAL '1 day',
                if_not_exists => TRUE);
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS sensor_data_measurement_time_idx "
            "ON sensor_data (measurement, time DESC);"
        )
    conn.commit()
    logger.info("Schema ready (hypertable sensor_data).")


def set_retention(conn, days: int) -> None:
    """Auto-drop chunks older than `days`. Re-running updates the policy."""
    with conn.cursor() as cur:
        cur.execute("SELECT remove_retention_policy('sensor_data', if_exists => TRUE);")
        cur.execute(
            "SELECT add_retention_policy('sensor_data', %s::interval);",
            (f"{days} days",),
        )
    conn.commit()
    logger.info("Retention policy set: data older than %d days is dropped automatically.", days)


def replace_window(conn, start: datetime, stop: datetime, records: list) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM sensor_data WHERE time >= %s AND time < %s", (start, stop)
            )
            if records:
                extras.execute_values(
                    cur,
                    "INSERT INTO sensor_data (time, measurement, data) VALUES %s",
                    [(t, m, extras.Json(d)) for t, m, d in records],
                    page_size=1000,
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def count_rows(conn, start: datetime, stop: datetime) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM sensor_data WHERE time >= %s AND time < %s",
            (start, stop),
        )
        return cur.fetchone()[0]


# --------------------------------- Main ------------------------------------ #
def main() -> int:
    parser = argparse.ArgumentParser(description="Raw InfluxDB -> TimescaleDB copy")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--start", help="UTC date, e.g. 2026-08-01 (overrides --days)")
    parser.add_argument("--end", help="UTC date, exclusive (default: now)")
    parser.add_argument(
        "--retention-days", type=int, default=30,
        help="Auto-delete data older than this many days (0 = keep forever)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Read Influx, skip DB")
    args = parser.parse_args()

    env_file = Path(__file__).resolve().parent / ".env"
    if env_file.exists():
        load_dotenv(env_file)

    end = _parse_date(args.end) if args.end else datetime.now(timezone.utc)
    start = _parse_date(args.start) if args.start else end - timedelta(days=args.days)
    logger.info("Migrating %s -> %s", start, end)

    influx = make_influx_client()
    conn = None if args.dry_run else connect_pg()

    total_fetched = 0
    try:
        if conn:
            create_schema(conn)
            if args.retention_days > 0:
                set_retention(conn, args.retention_days)

        cursor = start
        while cursor < end:
            stop = min(cursor + WINDOW, end)
            records = fetch_window(influx, cursor, stop)
            if conn:
                replace_window(conn, cursor, stop, records)
            total_fetched += len(records)
            logger.info("%s  copied %6d rows", cursor.date(), len(records))
            cursor = stop

        if total_fetched == 0:
            logger.error("No data returned from InfluxDB for that range.")
            return 1

        if conn:
            in_db = count_rows(conn, start, end)
            logger.info("Influx rows=%d  Timescale rows=%d", total_fetched, in_db)
            if in_db != total_fetched:
                logger.error("Row count mismatch.")
                return 2
    finally:
        influx.close()
        if conn:
            conn.close()

    logger.info("Migration completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())