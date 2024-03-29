-- Databricks notebook source
CREATE LIVE TABLE RunningAggregatesOrderBook_gold
USING DELTA
AS 
WITH AggregatedOrders AS (
   SELECT 
    ROW_NUMBER() OVER (PARTITION BY E.Name, P.Name, PI.Time, OT.Type ORDER BY CASE WHEN OT.Type = 'Bid' THEN Price END DESC, CASE WHEN OT.Type = 'Ask' THEN Price END ASC) AS RowNum,
    E.Name AS Exchange, 
    P.Name AS Product, 
    PI.Time,
    OT.Type AS OrderType,
    OB.Price, 
    OB.TotalQty,
    OB.NumOrders,
    ROUND(AVG(Price) OVER (PARTITION BY E.Name, P.Name, PI.Time, OT.Type ORDER BY CASE WHEN OT.Type = 'Bid' THEN Price END DESC, CASE WHEN OT.Type = 'Ask' THEN Price END ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS AvgPrice,
    ROUND(SUM(TotalQty * Price) OVER (PARTITION BY E.Name, P.Name, PI.Time, OT.Type ORDER BY CASE WHEN OT.Type = 'Bid' THEN Price END DESC, CASE WHEN OT.Type = 'Ask' THEN Price END ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS CumulativeAmount,
    ROUND(SUM(TotalQty) OVER (PARTITION BY E.Name, P.Name, PI.Time, OT.Type ORDER BY CASE WHEN OT.Type = 'Bid' THEN Price END DESC, CASE WHEN OT.Type = 'Ask' THEN Price END ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 4) AS CumulativeTotalQty,
    SUM(NumOrders) OVER (PARTITION BY E.Name, P.Name, PI.Time, OT.Type ORDER BY CASE WHEN OT.Type = 'Bid' THEN Price END DESC, CASE WHEN OT.Type = 'Ask' THEN Price END ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS CumulativeNumOrders
FROM 
    deltalive_gold.factOrderBook_gold AS OB
LEFT OUTER JOIN deltalive_gold.dimExchange_gold AS E ON E.Id = OB.ExchangeId
LEFT OUTER JOIN deltalive_gold.dimProduct_gold AS P ON P.Id = OB.ProductId
LEFT OUTER JOIN deltalive_gold.dimOrderType_gold AS OT ON OT.Id = OB.OrderTypeId
LEFT OUTER JOIN deltalive_gold.dimPollInfo_gold AS PI ON PI.Id = OB.PollInfoId
WHERE OT.Type IN ('Bid', 'Ask')
GROUP BY 
    E.Name, P.Name, PI.Time, OT.Type, OB.Price, OB.TotalQty, OB.NumOrders
)
SELECT * FROM AggregatedOrders
WHERE RowNum <= 1000;


-- COMMAND ----------


