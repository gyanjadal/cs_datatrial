-- Databricks notebook source
CREATE STREAMING LIVE TABLE orderbook_silver
USING DELTA
AS 
SELECT *, OrderArray[0] AS Price, OrderArray[1] AS TotalQty, OrderArray[2] AS NumOrders
FROM (
    SELECT *, split(regexp_replace(RawOrder, '([\[\"\])', ''), ',') AS OrderArray
    FROM (
        SELECT
            Exchange,
            Product,
            Time,
            Sequence,
            'Bid' AS OrderType,
            explode(split(bids, '],')) AS RawOrder
        FROM
            deltalive_bronze.orderbook_bronze
        UNION ALL
         SELECT
            Exchange,
            Product,
            Time,
            Sequence,
            'Ask' AS OrderType,
            explode(split(asks, '],')) AS RawOrder
        FROM
            deltalive_bronze.orderbook_bronze
    )
)

-- COMMAND ----------


