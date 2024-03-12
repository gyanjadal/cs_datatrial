-- Databricks notebook source
CREATE STREAMING LIVE TABLE orderbook_coinbase_bronze
USING DELTA
AS 
SELECT 
    'Coinbase' AS exchange,
    *
FROM 
(
    (
        SELECT 
            'ETH-USD' AS product, 
            bids,
            asks,
            pollTime as time,
            sequence
        FROM cloud_files ('s3://gj-ingestion-bucket/Exchanges/Coinbase/ETH-USD', 'json', map("inferSchema", "true"))
    )
UNION ALL
    (
        SELECT 
            'BTC-USD' AS product, 
            bids,
            asks,
            pollTime as time,
            sequence
        FROM cloud_files ('s3://gj-ingestion-bucket/Exchanges/Coinbase/BTC-USD', 'json', map("inferSchema", "true"))
    )
);

-- COMMAND ----------


