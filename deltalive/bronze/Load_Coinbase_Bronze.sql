-- Databricks notebook source
CREATE STREAMING LIVE TABLE orderbook_coinbase_bronze
USING DELTA
AS 
(
    (SELECT 
        'Coinbase' AS exchange, 
        'ETH-USD' AS product, 
        bids,
        asks,
        time,
        sequence
    FROM cloud_files ('s3://gj-ingestion-bucket/Coinbase/ETH-USD', 'json', map("inferSchema", "true")))
UNION ALL
    (SELECT 
        'Coinbase' AS exchange, 
        'BTC-USD' AS product, 
        bids,
        asks,
        time,
        sequence
    FROM cloud_files ('s3://gj-ingestion-bucket/Coinbase/BTC-USD', 'json', map("inferSchema", "true")))
);

-- COMMAND ----------


