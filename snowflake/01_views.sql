-- 01_views.sql
-- Reporting views for dashboards (pipeline health + stock analytics)

USE ROLE ROLE_STREAMING_PIPELINE;
USE WAREHOUSE WH_STREAMING_XS;
USE DATABASE STOCK_STREAMING;

-- Ensure MART schema exists (idempotent)
CREATE SCHEMA IF NOT EXISTS MART;

-- Pipeline metrics view: minute-level ingestion stats
CREATE OR REPLACE VIEW MART.VW_PIPELINE_MINUTE_METRICS AS
SELECT
  symbol,
  DATE_TRUNC('minute', end_ts) AS minute_ts,
  COUNT(*)                       AS bars_in_batch,
  MAX(ingest_ts)                 AS last_ingest_ts,
  MAX(end_ts)                    AS last_bar_end_ts,
  DATEDIFF('millisecond', MAX(end_ts), MAX(ingest_ts)) AS latency_ms,
  SUM(volume)                    AS volume_1m,
  SUM(notional)                  AS notional_1m
FROM CORE.FACT_TICKS_60S
GROUP BY symbol, DATE_TRUNC('minute', end_ts);

-- Five-minute rollup view for convenience (optional mart layer)
CREATE OR REPLACE VIEW MART.VW_PIPELINE_5M AS
SELECT
  symbol,
  start_ts,
  end_ts,
  MINUTE_COUNT,
  SOURCE_COUNT,
  VOLUME,
  NOTIONAL,
  INGEST_TS
FROM CORE.FACT_TICKS_300S;

-- Stock analytics view joining symbol metadata
CREATE OR REPLACE VIEW MART.VW_PRICE_60S AS
SELECT
  f.symbol,
  f.start_ts,
  f.end_ts,
  f.open,
  f.high,
  f.low,
  f.close,
  f.volume,
  f.notional,
  f.ingest_ts,
  s.NAME        AS company_name,
  s.EXCHANGE    AS exchange,
  s.SECTOR      AS sector,
  s.INDUSTRY    AS industry
FROM CORE.FACT_TICKS_60S f
LEFT JOIN CORE.DIM_SYMBOL s ON f.symbol = s.SYMBOL;

-- Rolling VWAP/volatility helper view (60-minute window)
CREATE OR REPLACE VIEW MART.VW_PRICE_60S_FEATURES AS
SELECT
  symbol,
  start_ts,
  close,
  AVG(close) OVER (
    PARTITION BY symbol
    ORDER BY start_ts
    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
  ) AS ma_60,
  STDDEV(close) OVER (
    PARTITION BY symbol
    ORDER BY start_ts
    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
  ) AS std_60,
  SUM(notional) OVER (
    PARTITION BY symbol
    ORDER BY start_ts
    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
  ) / NULLIF(SUM(volume) OVER (
    PARTITION BY symbol
    ORDER BY start_ts
    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
  ), 0) AS rolling_vwap
FROM CORE.FACT_TICKS_60S;

-- Session VWAP per day (for dashboard scorecards)
CREATE OR REPLACE VIEW MART.VW_SESSION_VWAP AS
SELECT
  symbol,
  DATE_TRUNC('day', start_ts) AS trade_date,
  SUM(notional) / NULLIF(SUM(volume), 0) AS session_vwap,
  SUM(volume) AS session_volume
FROM CORE.FACT_TICKS_60S
GROUP BY symbol, DATE_TRUNC('day', start_ts);

-- Top movers using latest five-minute bars (same as mart view but materialized here)
CREATE OR REPLACE VIEW MART.VW_TOP_MOVERS_15M AS
SELECT
  symbol,
  MAX(end_ts) AS last_bar_end,
  (MAX(close) - MIN(open)) / NULLIF(MIN(open), 0) AS pct_change,
  SUM(volume) AS total_volume
FROM CORE.FACT_TICKS_300S
WHERE end_ts >= DATEADD('minute', -15, CURRENT_TIMESTAMP())
GROUP BY symbol
ORDER BY pct_change DESC;

