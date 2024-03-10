-- Databricks notebook source
CREATE STREAMING LIVE TABLE orderbook_bronze
USING DELTA
AS SELECT 
    'Coinbase' AS Exchange, 
    'ETH-USD' AS Product, 
    * 
FROM cloud_files ('s3://gj-ingestion-bucket/Coinbase/ETH-USD', 'json', map("inferSchema", "true"));

-- COMMAND ----------

CREATE STREAMING LIVE TABLE orderbook_bronze
USING DELTA
AS SELECT 
    'Coinbase' AS Exchange, 
    'BTC-USD' AS Product, 
    * 
FROM cloud_files ('s3://gj-ingestion-bucket/Coinbase/BTC-USD', 'json', map("inferSchema", "true"));
