# InfluxDB to TimescaleDB migration

A one-shot Python migration that copies raw InfluxDB data into a TimescaleDB hypertable.

Each InfluxDB point is stored in `sensor_data` with its timestamp, measurement name, and every field/tag in a `JSONB` column. The migration works in one-day windows, replacing the destination data for each window, so it is safe to re-run without creating duplicates.

## Requirements

- Python 3.10+
- Access to the source InfluxDB bucket
- A TimescaleDB instance (or Docker for the included local instance)

## Set up

1. Create and activate a virtual environment.

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

2. Install the dependencies.

   ```bash
   pip install -r requirements.txt
   ```

3. Create a `.env` file beside `migration.py`.

   ```dotenv
   INFLUX_URL=https://your-influx-host
   INFLUX_TOKEN=your-token
   INFLUX_ORG=your-org
   INFLUX_BUCKET=your-bucket
   INFLUX_VERIFY_SSL=true

   PG_HOST=localhost
   PG_PORT=5433
   PG_DATABASE=intellisenz
   PG_USER=postgres
   PG_PASSWORD=postgres
   PG_SSLMODE=prefer
   ```

   `INFLUX_VERIFY_SSL` defaults to `false`. Set it to `true` when the InfluxDB server has a valid TLS certificate.

## Start TimescaleDB locally

The included Compose service exposes PostgreSQL on port `5433` and persists its data in the `timescaledb_data` Docker volume.

```bash
docker compose up -d
```

Its default local credentials are `postgres` / `postgres`; update `docker-compose.yml` before use outside local development.

## Run the migration

```bash
# Copy the most recent 30 days (default)
python migration.py

# Copy a rolling range
python migration.py --days 90

# Copy an explicit UTC range; --end is exclusive
python migration.py --start 2026-08-01 --end 2026-09-01

# Validate what InfluxDB returns without writing to TimescaleDB
python migration.py --dry-run
```

By default, the script sets a 30-day TimescaleDB retention policy. Override it or disable it:

```bash
python migration.py --retention-days 60
python migration.py --retention-days 0
```

## What the script does

For every one-day window in the requested range, it:

1. Fetches the bucket data from InfluxDB.
2. Deletes destination rows in that same time range.
3. Inserts the fetched records in one transaction.
4. At completion, compares the total fetched rows with rows in TimescaleDB.

The destination schema is created automatically:

```text
sensor_data(
  time TIMESTAMPTZ,
  measurement TEXT,
  data JSONB
)
```

`sensor_data` is a TimescaleDB hypertable partitioned into one-day chunks, with an index on `(measurement, time DESC)`.

## Notes

- Dates passed to `--start` and `--end` are interpreted as UTC.
- An empty source range exits with a non-zero status.
- The migration preserves raw fields and tags in `data`; it does not normalize measurements into separate tables.
