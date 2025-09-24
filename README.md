# Stock Streaming Pipeline

Real-time pipeline that replays historical tick data, publishes 10 ms bins into Kafka, aggregates 1 s OHLCV bars, and stages downstream delivery to Snowflake.

## Architecture
```
                        +---------------------+
                        |  Mock Tick API      |
                        |  (api/mock_tick_api)|
                        +----------+----------+
                                   |
                     HTTP NDJSON   |
                                   v
+----------------+        +--------+--------+       Kafka Topic      +-----------------+
| data/input/    | -----> | Replay Producer | ---------------------> | ticks.raw (10ms)|
| historical txt |        | (producers/)    |                        +-----------------+
+----------------+        +--------+--------+                                 |
                                          Kafka                               v
                                          |             Kafka Topic    +-------------------+
                                          +--------------------------> | ticks.agg.1s      |
                                                                       | (1 s OHLCV bars)  |
                                                                       +---------+---------+
                                                                                 |
                                                                                 v
                                                       Minute micro-batch loader (Snowflake)
```

## Repository Layout
- `api/` - mock HTTP source that replays staged ticks as NDJSON.
- `data/input/` - drop large historical files here (ignored by git).
- `data/samples/` - small curated samples for local testing.
- `docs/contracts/` - JSON schemas for Kafka topics and processing notes.
- `docs/` - processing strategy, SLAs, Snowflake setup, and operational docs.
- `docker/` - Docker Compose stack for Kafka + UI.
- `producers/` - replay and live producers that publish into Kafka.
- `processors/` - stream processors (1 s aggregator).
- `consumers/` - downstream consumers (Snowflake loaders, etc.).
- `dashboard/` - Streamlit real-time dashboard fed from ticks.agg.1s.
- `ops/` - helper scripts and runbooks.
- `scripts/` - one-off data utilities.

## Getting Started
1. Copy `.env.example` to `.env` and adjust any overrides (Kafka broker, mock API, Snowflake placeholders).
2. Create a virtual environment and install deps:
   - `python -m venv .venv`
   - `.\.venv\Scripts\Activate.ps1`
   - `pip install -r requirements.txt`
3. Stage data: download `WDC_tickbidask.txt` (or another dataset) into `data/input/`.
4. Start infrastructure: `make up`.
5. Bootstrap topics and run smoke test: `make smoke`.
6. Launch the mock API (`python api/mock_tick_api.py`) and the replay producer (`make replay`).
7. Start the 1 s aggregator via `make aggregate`.
8. Launch the real-time dashboard: `make dashboard`. This sets `DASHBOARD_ENV_FILE` so the Streamlit app picks up `.env` overrides.
9. (Optional) Start the Snowflake micro-batch loader once Snowflake creds are in `.env`: `make snowflake-loader`.

## Dashboard and Monitoring
- The Streamlit app (`dashboard/app.py`) consumes `ticks.agg.1s` through an in-memory cache (`dashboard/cache.py`).
- Candlestick view renders 1 s OHLCV bars with a 60-900 s history window per symbol.
- Sidebar badges display end-to-end latency (`ingest_ts_ms - end_ts_ms`), Kafka log lag (consumer receive time vs. broker timestamp), data freshness, cached bar count, and last update.
- Auto-refresh uses Streamlit's in-session rerun (toggleable in the sidebar); it avoids full page reloads so filters stay put.
- Capture a proof screenshot by running a replay sample, opening the dashboard, and using your OS tooling once metrics settle below targets.

## Snowflake Warehouse
- Snowflake bootstrap DDL, stage/table definitions, and dimension seeding live in `snowflake/00_setup.sql`.
- Apply it via SnowSQL (replace placeholders): `snowsql -a <account> -u <admin_user> -f snowflake/00_setup.sql`. The script creates `ROLE_STREAMING_PIPELINE`, `WH_STREAMING_XS`, RAW/CORE/MART schemas, and seeds `CORE.DIM_SYMBOL` + `CORE.DIM_CALENDAR` for the current year.
- Configure credentials in `.env` (`SNOW_ACCOUNT`, `SNOW_USER`, `SNOW_PASSWORD`, `SNOW_WAREHOUSE`, `SNOW_DATABASE`, `SNOW_SCHEMA`).
- Run `make snowflake-loader` to stream 5-row micro-batches every five minutes. Each batch inserts five 1-minute snapshots (open/high/low/close/volume) and a consolidated five-minute fact into `CORE.FACT_TICKS_60S` and `CORE.FACT_TICKS_300S`.
- XS warehouse with auto-suspend keeps credit usage tiny; batches execute in seconds and the warehouse sleeps again.

## Validation Toolkit
- `scripts/validate_aggregates.py` recalculates 1 s bars from replayed 10 ms bins and compares them with the processor output.
- Typical workflow:
  1. Run the replay producer with Kafka disabled to emit `logs/replay.jsonl`.
  2. Run the aggregator (with Kafka disabled) to emit `logs/aggregate.jsonl`.
  3. Execute `python scripts/validate_aggregates.py --bins logs/replay.jsonl --bars logs/aggregate.jsonl`.
  4. The script reports missing/extra bars, field diffs, and latency stats (p95/max) to confirm correctness before a full pipeline run.

## Contracts & Docs
- Raw 10 ms bin schema: `docs/contracts/ticks_raw_schema.json`.
- Aggregated 1 s bar schema: `docs/contracts/ticks_agg_1s_schema.json`.
- Processing strategy, SLAs, and Snowflake runbook: `docs/processing_strategy.md`.

## Notes
- Kafka UI is available at `http://localhost:8080` once the stack is running.
- Large input files are ignored by git - add them locally under `data/input/`.
- All Make targets read configuration from `.env` by default (`ENV_FILE` override available).
- The dashboard uses the same `.env` defaults but respects `DASHBOARD_*` overrides for cache sizing or Kafka group IDs when needed.

## End-to-end Pipeline
- Ensure Kafka is up (make up) and Snowflake credentials are set in .env.
- Launch everything from one shell with make pipeline. This starts the mock API, replay producer, 1 s aggregator, and (if credentials are present) the Snowflake micro-batch loader.
- Use Ctrl+C to stop; the launcher sends interrupts to each child process.
- Toggle auto-refresh in the dashboard sidebar to keep the Streamlit UI responsive while aggregates flow to Snowflake.

Kafka topics are configured for short retention (KAFKA_CFG_LOG_RETENTION_MINUTES=60 in docker/compose.yml) so raw ticks age out after an hour, keeping storage small while the Snowflake loader persists only the 1- and 5-minute facts.



