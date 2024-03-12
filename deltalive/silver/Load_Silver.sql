-- Databricks notebook source
CREATE STREAMING LIVE TABLE orderbook_silver
    
USING DELTA
AS 
SELECT 
    Exchange::STRING,
    Product::STRING,
    Time::TIMESTAMP,
    Sequence::LONG,
    OrderType::STRING,
    OrderArray[0]::DOUBLE AS Price, 
    OrderArray[1]::DOUBLE AS TotalQty, 
    OrderArray[2]::Int AS NumOrders
FROM (
    SELECT 
        *, 
        split(replace(replace(replace(RawOrder, '[', ''), ']', ''), '"', ''), ',') AS OrderArray
    FROM (
        SELECT
            Exchange,
            Product,
            Time,
            Sequence,
            'Bid' AS OrderType,
            explode(split(bids, '],')) AS RawOrder
        FROM
            STREAM (deltalive_bronze.orderbook_bronze)
        UNION ALL
         SELECT
            Exchange,
            Product,
            Time,
            Sequence,
            'Ask' AS OrderType,
            explode(split(asks, '],')) AS RawOrder
        FROM
            STREAM (deltalive_bronze.orderbook_bronze)
    )
)

-- COMMAND ----------


