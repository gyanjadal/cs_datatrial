-- Databricks notebook source
CREATE STREAMING LIVE TABLE orderbook_kraken_bronze
USING DELTA
AS 
SELECT 
    'Kraken' AS exchange, 
    product,
    bids,
    asks,
    time,
    0 AS sequence
FROM
(
    (
        SELECT 
        'ETH-USD' AS product, 
        result:XETHZUSD['bids'] AS bids,
        result:XETHZUSD['asks'] AS asks,
        pollTime AS time
        FROM cloud_files ('s3://gj-ingestion-bucket/Exchanges/Kraken/ETH-USD', 'json', map("inferSchema", "true"))
    )
UNION ALL
    (
        SELECT 
        'BTC-USD' AS product, 
        result:XXBTZUSD['bids'] AS bids,
        result:XXBTZUSD['asks'] AS asks,
        pollTime AS time
        FROM cloud_files ('s3://gj-ingestion-bucket/Exchanges/Kraken/BTC-USD', 'json', map("inferSchema", "true"))
    )
);

-- COMMAND ----------


