-- Databricks notebook source
CREATE STREAMING LIVE TABLE aggregateOrderBook_gold
USING DELTA
AS 
SELECT 
    SUM(Qty) OVER (ORDER BY <your_ordering_column>) AS cumQty,
    AVG(Price) OVER (ORDER BY <your_ordering_column> ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS avgPrice,
    SUM(Qty * Price) OVER (ORDER BY <your_ordering_column>) AS cumTotalPrice
FROM 
    STREAM ;
FROM
    deltalive_gold.factOrderBook_gold AS OB,
    deltalive_gold.dimExchange_gold AS E,
    deltalive_gold.dimProduct_gold AS P,
    deltalive_gold.dimOrderType_gold AS OT,
    deltalive_gold.dimPollInfo_gold AS PI


-- COMMAND ----------


