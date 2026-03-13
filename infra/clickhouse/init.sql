CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.customer_daily (
    day Date,
    customer_status LowCardinality(String),
    cnt UInt64
)
ENGINE = MergeTree
ORDER BY (day, customer_status);

