# Processing Strategy

## 10 ms Replay Producer
- **Source**: Streams NDJSON ticks from `MOCK_API_URL`.
- **Bin size**: `REPLAY_BIN_MS` (default 10 ms). All ticks whose event timestamp falls in the same bin share a deterministic `bin_id = <symbol>-<bin_start_epoch_ms>`.
- **Watermark**: Maintains the highest event timestamp seen per symbol. A bin closes once its end time is older than `current_watermark - REPLAY_MAX_LATE_MS` (default 500 ms).
- **Lateness handling**: Late ticks (<500 ms behind watermark) are merged into their original bin; later arrivals are logged to a dead-letter file and exposed via metrics.
- **Idempotency**: `bin_id` is used as the Kafka message key and in payload. Replays can safely re-send bins without creating duplicates downstream.

## 1 s Aggregator
- **Input topic**: `ticks.raw` (10 ms bins).
- **Windowing**: Tumbling 1 s windows per symbol. Each 10 ms bin is folded into its parent second using the bin `start_ts_ms`.
- **Outputs**: OHLC, total volume, notional, VWAP (`notional / volume`), contributing bin count, and max lateness propagated from inputs.
- **Watermark propagation**: Aggregator tracks the max `end_ts_ms` seen per symbol and emits a bar when `watermark - REPLAY_MAX_LATE_MS >= bar_end`.
- **Idempotency**: Bars keyed by `bar_id = <symbol>-<bar_start_epoch_ms>` to allow exactly-once semantics when brokers enable log compaction.

## Minute Micro-batch Loader (Snowflake)
- **Consumer**: `consumers/snowflake_loader_minute.py` reads `ticks.agg.1s` with group `snowflake-minute-loader`.
- **Minute rollups**: Bundles 1 s bars into 1-minute snapshots (open/high/low/close, volume, notional, contributing count).
- **Flush cadence**: Every five completed minute snapshots per symbol (default `SNOWFLAKE_FLUSH_MINUTES=5`), the loader upserts five rows into `CORE.FACT_TICKS_60S` and a consolidated five-minute record into `CORE.FACT_TICKS_300S`.
- **Idempotency**: Each row is keyed by `BAR_ID = <symbol>-<start_ms>-<window>`; the loader deletes any existing row before insert to keep Snowflake costs low and avoid duplicates.
- **Warehouse footprint**: XS warehouse with 60 s autosuspend resumes for a few seconds per micro-batch, keeping the free-trial credit burn minimal.

## Operational Metrics
- **Ingest latency**: Compare producer ingest timestamp vs Kafka log time via Kafka UI.
- **Backlog size**: Inspect consumer group lag for the aggregator or the Snowflake loader to ensure the micro-batch keeps up.
- **Dead-letter volume**: Tail `logs/replay_dlt.jsonl` (emitted when lateness exceeds `REPLAY_MAX_LATE_MS`).

## Serving Path & Dashboard
- **Cache**: `dashboard/cache.py` runs a dedicated Kafka consumer (`ticks.agg.1s`) that retains the latest 600 bars per symbol with latency snapshots.
- **UI**: Streamlit app (`dashboard/app.py`) renders candlesticks, latency/backlog badges, and latest bar details with an in-session auto-refresh toggle (default 1 s cadence).
- **Metrics**: End-to-end latency uses `ingest_ts_ms - end_ts_ms`, Kafka log lag uses consumer receive vs. broker timestamp, and freshness reflects dashboard staleness.

## Snowflake Warehouse Layout
- **RAW**: `RAW_STAGE` (GZIP NDJSON) and `RAW.TICKS_RAW` for provenance loads from the replay producer (hourly `COPY` batches).
- **CORE**: `FACT_TICKS_1S`, `FACT_TICKS_60S`, and `FACT_TICKS_300S` hold curated 1 s, 1 min, and 5 min facts keyed by `BAR_ID`.
- **Dimensions**: `DIM_SYMBOL` (reference data) and `DIM_CALENDAR` (minute-level trading calendar) seed downstream joins.
- **Marts**: Views such as `MART.VW_TOP_MOVERS_15M`, `MART.VW_SESSION_VWAP`, and `MART.VW_VOLATILITY_BANDS` showcase analytics without heavy compute.

## SLAs
- **Producer publish**: <20 ms from raw tick ingest to Kafka append (monitored via Kafka timestamps).
- **Processor latency**: <750 ms from bar end time to aggregator emit (`ingest_ts_ms`).
- **Dashboard freshness**: Auto-refresh every 1 s (toggleable) with backlog badge to ensure <1 s data staleness.
- **Snowflake micro-batch**: 5-minute flush within 30 s of window close; warehouse compute <60 seconds/hour on XS.

## Validation Workflow
1. Replay sample data with Kafka disabled to capture `logs/replay.jsonl` (10 ms bins).
2. Run the aggregator (also without Kafka) to capture `logs/aggregate.jsonl`.
3. Execute `python scripts/validate_aggregates.py --bins logs/replay.jsonl --bars logs/aggregate.jsonl`.
4. Investigate any missing/extra bars or field mismatches; latency p95/max outputs highlight SLA regressions before a live run.

## Retention
- Kafka topics are configured with a 60 minute log retention (see docker/compose.yml). Raw 10 ms bins age out quickly after the dashboard and aggregators consume them.
- Snowflake stores only the curated 1 minute and 5 minute facts (minute micro-batch loader) along with dimensional reference data. Raw NDJSON files should only be staged temporarily for replay diagnostics.
\n## Reporting Views\n- Dashboard-friendly views live in snowflake/01_views.sql. Apply it after the base setup to expose pipeline metrics (MART.VW_PIPELINE_MINUTE_METRICS), one-hour price views, and helper features (rolling VWAP, top movers).\n- Snowsight dashboards can query these views directly without touching core tables.\n
