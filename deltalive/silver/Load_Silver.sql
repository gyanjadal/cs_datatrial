-- Databricks notebook source
CREATE STREAMING LIVE TABLE orderbook_silver
    
USING DELTA
AS 
SELECT 
    exchange::STRING,
    product::STRING,
    time::TIMESTAMP,
    sequence::LONG,
    orderType::STRING,
    bidsOrAsksArray[0]::DOUBLE AS price, 
    bidsOrAsksArray[1]::DOUBLE AS totalQty, 
    bidsOrAsksArray[2]::Int AS numOrders
FROM (
    SELECT 
        *, 
        split(replace(replace(replace(rawBidsOrAsks, '[', ''), ']', ''), '"', ''), ',') AS bidsOrAsksArray
    FROM (
        SELECT
            exchange,
            product,
            time,
            sequence,
            'Bid' AS orderType,
            explode(split(bids, '],')) AS rawBidsOrAsks
        FROM
            STREAM (deltalive_bronze.orderbook_coinbase_bronze)
        UNION ALL
         SELECT
            exchange,
            product,
            time,
            sequence,
            'Ask' AS orderType,
            explode(split(asks, '],')) AS rawBidsOrAsks
        FROM
            STREAM (deltalive_bronze.orderbook_coinbase_bronze)
        UNION ALL
        SELECT
            exchange,
            product,
            time,
            sequence,
            'Bid' AS orderType,
            explode(split(bids, '],')) AS rawBidsOrAsks
        FROM
            STREAM (deltalive_bronze.orderbook_kraken_bronze)
        UNION ALL
         SELECT
            exchange,
            product,
            time,
            sequence,
            'Ask' AS orderType,
            explode(split(asks, '],')) AS rawBidsOrAsks
        FROM
            STREAM (deltalive_bronze.orderbook_kraken_bronze)
    )
)

-- COMMAND ----------


