-- Databricks notebook source
CREATE STREAMING LIVE TABLE factOrderBook_gold
USING DELTA
AS 
SELECT
    uuid() AS Id,
    E.Id AS ExchangeId,
    P.Id AS ProductId,
    OT.Id AS OrderTypeId,
    PI.Id AS PollInfoId,
    OB.Price,
    OB.TotalQty,
    OB.NumOrders
FROM
    STREAM (deltalive_silver.orderbook_silver) AS OB,
    deltalive_gold.dimExchange_gold AS E,
    deltalive_gold.dimProduct_gold AS P,
    deltalive_gold.dimOrderType_gold AS OT,
    deltalive_gold.dimPollInfo_gold AS PI
WHERE E.Name = OB.Exchange 
  AND P.Name = OB.Product
  AND OT.Type = OB.OrderType
  AND PI.Time = OB.Time

-- COMMAND ----------


