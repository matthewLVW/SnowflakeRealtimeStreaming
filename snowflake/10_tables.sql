-- Table definitions for realtime quote ingestion.
CREATE OR REPLACE TABLE quotes (
    symbol STRING,
    price FLOAT,
    ts TIMESTAMP_NTZ
);
