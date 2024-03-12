-- Databricks notebook source
CREATE STREAMING LIVE TABLE dimExchange_gold
USING DELTA
AS 
SELECT
    uuid() AS Id,
    Exchange AS Name
FROM
    STREAM (deltalive_silver.orderbook_silver)
GROUP BY Exchange


-- COMMAND ----------

CREATE STREAMING LIVE TABLE dimProduct_gold
USING DELTA
AS 
SELECT
    uuid() AS Id,
    Product AS Name
FROM
    STREAM (deltalive_silver.orderbook_silver)
GROUP BY Product


-- COMMAND ----------

CREATE STREAMING LIVE TABLE dimOrderType_gold
USING DELTA
AS 
SELECT
    uuid() AS Id,
    OrderType AS Type
FROM
    STREAM (deltalive_silver.orderbook_silver)
GROUP BY OrderType

-- COMMAND ----------

CREATE STREAMING LIVE TABLE dimPollInfo_gold
USING DELTA
AS 
SELECT
    uuid() AS Id,
    Time,
    Sequence
FROM
    STREAM (deltalive_silver.orderbook_silver)
GROUP BY Time, Sequence

-- COMMAND ----------


