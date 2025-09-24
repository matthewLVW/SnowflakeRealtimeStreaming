-- Day 4 : Snowflake bootstrap + dimension seeding
-- Execute with ACCOUNTADMIN (or equivalent) for role/warehouse creation, then rerun the lower
-- section as ROLE_STREAMING_PIPELINE to seed reference data. Example:
--   snowsql -a <account> -u <admin_user> -f snowflake/00_setup.sql

/* -------------------------------------------------------------------------- */
/* Foundation roles + warehouse                                               */
/* -------------------------------------------------------------------------- */

CREATE OR REPLACE ROLE ROLE_STREAMING_PIPELINE;
-- Grant the new role to SYSADMIN so platform admins retain control
GRANT ROLE ROLE_STREAMING_PIPELINE TO ROLE SYSADMIN;
-- TODO: Grant to specific users, e.g.: GRANT ROLE ROLE_STREAMING_PIPELINE TO USER <your_user>;

CREATE OR REPLACE WAREHOUSE WH_STREAMING_XS
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Streaming tick pipeline micro-batches';
GRANT USAGE ON WAREHOUSE WH_STREAMING_XS TO ROLE ROLE_STREAMING_PIPELINE;

CREATE OR REPLACE DATABASE STOCK_STREAMING;
GRANT OWNERSHIP ON DATABASE STOCK_STREAMING TO ROLE ROLE_STREAMING_PIPELINE;

/* -------------------------------------------------------------------------- */
/* Logical environment                                                        */
/* -------------------------------------------------------------------------- */

USE ROLE ROLE_STREAMING_PIPELINE;
USE DATABASE STOCK_STREAMING;
USE WAREHOUSE WH_STREAMING_XS;

CREATE OR REPLACE SCHEMA RAW;
CREATE OR REPLACE SCHEMA CORE;
CREATE OR REPLACE SCHEMA MART;

/* -------------------------------------------------------------------------- */
/* RAW landing layer                                                          */
/* -------------------------------------------------------------------------- */

USE SCHEMA RAW;

CREATE OR REPLACE STAGE RAW_STAGE
  FILE_FORMAT = (TYPE = JSON, STRIP_OUTER_ARRAY = FALSE, COMPRESSION = GZIP)
  COMMENT = 'Landing stage for compressed NDJSON ticks';
GRANT USAGE ON STAGE RAW_STAGE TO ROLE ROLE_STREAMING_PIPELINE;

CREATE OR REPLACE TABLE TICKS_RAW (
  RECORD VARIANT,
  LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

/* -------------------------------------------------------------------------- */
/* CORE fact tables + dimensions                                              */
/* -------------------------------------------------------------------------- */

USE SCHEMA CORE;

CREATE OR REPLACE TABLE FACT_TICKS_1S (
  BAR_ID STRING PRIMARY KEY,
  SYMBOL STRING,
  START_TS TIMESTAMP_NTZ,
  END_TS TIMESTAMP_NTZ,
  OPEN NUMBER(18,6),
  HIGH NUMBER(18,6),
  LOW NUMBER(18,6),
  CLOSE NUMBER(18,6),
  VOLUME NUMBER(18,6),
  NOTIONAL NUMBER(18,6),
  VWAP NUMBER(18,6),
  BIN_COUNT NUMBER(10,0),
  MAX_LATENESS_MS NUMBER(10,0),
  INGEST_TS TIMESTAMP_NTZ,
  LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE FACT_TICKS_60S (
  BAR_ID STRING PRIMARY KEY,
  SYMBOL STRING,
  START_TS TIMESTAMP_NTZ,
  END_TS TIMESTAMP_NTZ,
  OPEN NUMBER(18,6),
  CLOSE NUMBER(18,6),
  HIGH NUMBER(18,6),
  LOW NUMBER(18,6),
  VOLUME NUMBER(18,6),
  NOTIONAL NUMBER(18,6),
  INGEST_TS TIMESTAMP_NTZ,
  SOURCE_COUNT NUMBER(10,0),
  MINUTE_INDEX NUMBER(5,0),
  LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE FACT_TICKS_300S (
  BAR_ID STRING PRIMARY KEY,
  SYMBOL STRING,
  START_TS TIMESTAMP_NTZ,
  END_TS TIMESTAMP_NTZ,
  OPEN NUMBER(18,6),
  CLOSE NUMBER(18,6),
  HIGH NUMBER(18,6),
  LOW NUMBER(18,6),
  VOLUME NUMBER(18,6),
  NOTIONAL NUMBER(18,6),
  INGEST_TS TIMESTAMP_NTZ,
  MINUTE_COUNT NUMBER(5,0),
  SOURCE_COUNT NUMBER(10,0),
  LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE DIM_SYMBOL (
  SYMBOL STRING PRIMARY KEY,
  NAME STRING,
  EXCHANGE STRING,
  SECTOR STRING,
  INDUSTRY STRING,
  IS_ACTIVE BOOLEAN DEFAULT TRUE,
  LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE DIM_CALENDAR (
  CALENDAR_MINUTE TIMESTAMP_NTZ PRIMARY KEY,
  TRADE_DATE DATE,
  WEEKDAY NUMBER(1,0),
  HOUR NUMBER(2,0),
  MINUTE NUMBER(2,0),
  IS_TRADING_SESSION BOOLEAN,
  SESSION_NAME STRING,
  LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

/* -------------------------------------------------------------------------- */
/* Seed dimensions                                                            */
/* -------------------------------------------------------------------------- */

TRUNCATE TABLE DIM_SYMBOL;
INSERT INTO DIM_SYMBOL (SYMBOL, NAME, EXCHANGE, SECTOR, INDUSTRY)
VALUES
  ('WDC', 'Western Digital Corporation', 'NASDAQ', 'Technology', 'Computer Hardware'),
  ('AAPL', 'Apple Inc.', 'NASDAQ', 'Technology', 'Consumer Electronics'),
  ('MSFT', 'Microsoft Corporation', 'NASDAQ', 'Technology', 'Software'),
  ('TSLA', 'Tesla Inc.', 'NASDAQ', 'Consumer Discretionary', 'Automobiles');

-- Calendar seed: trading minutes for the current year (UTC timestamps)
TRUNCATE TABLE DIM_CALENDAR;
SET START_TS_UTC = TO_TIMESTAMP_NTZ('2025-01-01 00:00:00');
SET END_TS_UTC   = TO_TIMESTAMP_NTZ('2025-12-31 23:59:00');

INSERT INTO DIM_CALENDAR (CALENDAR_MINUTE, TRADE_DATE, WEEKDAY, HOUR, MINUTE, IS_TRADING_SESSION, SESSION_NAME)
WITH minute_series AS (
  SELECT DATEADD(minute, SEQ4(), $START_TS_UTC) AS calendar_minute
  FROM TABLE(GENERATOR(ROWCOUNT => 600000))
), trimmed AS (
  SELECT calendar_minute
  FROM minute_series
  WHERE calendar_minute <= $END_TS_UTC
)
SELECT
  calendar_minute,
  DATE_TRUNC('day', calendar_minute) AS trade_date,
  DAYOFWEEKISO(calendar_minute) AS weekday,
  EXTRACT(hour FROM calendar_minute) AS hour,
  EXTRACT(minute FROM calendar_minute) AS minute,
  CASE
    WHEN DAYOFWEEKISO(calendar_minute) BETWEEN 1 AND 5
         AND EXTRACT(hour FROM calendar_minute) BETWEEN 13 AND 19
         AND NOT (EXTRACT(hour FROM calendar_minute) = 13 AND EXTRACT(minute FROM calendar_minute) < 30)
         AND NOT (EXTRACT(hour FROM calendar_minute) = 19 AND EXTRACT(minute FROM calendar_minute) > 59)
      THEN TRUE
    ELSE FALSE
  END AS is_trading_session,
  CASE
    WHEN DAYOFWEEKISO(calendar_minute) BETWEEN 1 AND 5
         AND EXTRACT(hour FROM calendar_minute) BETWEEN 13 AND 19
         AND NOT (EXTRACT(hour FROM calendar_minute) = 13 AND EXTRACT(minute FROM calendar_minute) < 30)
         AND NOT (EXTRACT(hour FROM calendar_minute) = 19 AND EXTRACT(minute FROM calendar_minute) > 59)
      THEN 'REG'
    ELSE 'OFF'
  END AS session_name
FROM trimmed;

/* -------------------------------------------------------------------------- */
/* Marts (views only, no additional storage)                                  */
/* -------------------------------------------------------------------------- */

USE SCHEMA MART;

CREATE OR REPLACE VIEW VW_TOP_MOVERS_15M AS
SELECT
  symbol,
  MAX(end_ts) AS last_bar_end,
  (MAX(close) - MIN(open)) / NULLIF(MIN(open), 0) AS pct_change,
  SUM(volume) AS total_volume
FROM CORE.FACT_TICKS_300S
WHERE end_ts >= DATEADD('minute', -15, CURRENT_TIMESTAMP())
GROUP BY symbol
ORDER BY pct_change DESC;

CREATE OR REPLACE VIEW VW_SESSION_VWAP AS
SELECT
  symbol,
  DATE_TRUNC('day', start_ts) AS trade_date,
  SUM(notional) / NULLIF(SUM(volume), 0) AS session_vwap
FROM CORE.FACT_TICKS_60S
GROUP BY symbol, DATE_TRUNC('day', start_ts);

CREATE OR REPLACE VIEW VW_VOLATILITY_BANDS AS
SELECT
  symbol,
  start_ts,
  close,
  AVG(close) OVER (PARTITION BY symbol ORDER BY start_ts ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma_60,
  STDDEV(close) OVER (PARTITION BY symbol ORDER BY start_ts ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS std_60
FROM CORE.FACT_TICKS_60S;

-- Optional: GRANT SELECT ON ALL VIEWS IN SCHEMA MART TO ROLE ROLE_BI;
