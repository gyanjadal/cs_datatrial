-- Databricks notebook source
CREATE LIVE TABLE RunningAggregatesOrderBook_gold
USING DELTA
AS 
    SELECT * FROM 
        (SELECT 
        E.Name AS Exchange, 
        P.Name AS Product, 
        PI.Time,
        OT.Type AS OrderType,
        OB.Price, 
        OB.TotalQty,
        OB.NumOrders,
        ROW_NUMBER() OVER (PARTITION BY E.Name, P.Name, PI.Time, OT.Type ORDER BY Price DESC) AS row_num,
        ROUND(SUM(TotalQty) OVER (PARTITION BY E.Name, P.Name, PI.Time, OT.Type ORDER BY Price DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 3) AS CumulativeTotalQty,
        ROUND(SUM(TotalQty * Price) OVER (PARTITION BY E.Name, P.Name, PI.Time, OT.Type ORDER BY Price DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS CumulativeAmount,
        ROUND(AVG(Price) OVER (PARTITION BY E.Name, P.Name, PI.Time, OT.Type ORDER BY Price DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS AvgPrice,
        SUM(NumOrders) OVER (PARTITION BY E.Name, P.Name, PI.Time, OT.Type ORDER BY Price DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS CumulativeNumOrders
    FROM 
        (deltalive_gold.factOrderBook_gold) AS OB
    LEFT OUTER JOIN deltalive_gold.dimExchange_gold AS E ON E.Id = OB.ExchangeId
    LEFT OUTER JOIN deltalive_gold.dimProduct_gold AS P ON P.Id = OB.ProductId
    LEFT OUTER JOIN deltalive_gold.dimOrderType_gold AS OT ON OT.Id = OB.OrderTypeId
    LEFT OUTER JOIN deltalive_gold.dimPollInfo_gold AS PI ON PI.Id = OB.PollInfoId
    WHERE OT.Type = 'Bid'
    GROUP BY 
        E.Name, P.Name, PI.Time, OT.Type, OB.Price, OB.TotalQty, OB.NumOrders)
    WHERE row_num <= 1000
UNION ALL
    SELECT * FROM
        (SELECT 
        E.Name AS Exchange, 
        P.Name AS Product, 
        PI.Time,
        OT.Type AS OrderType,
        OB.Price, 
        OB.TotalQty,
        OB.NumOrders,
        ROW_NUMBER() OVER (PARTITION BY E.Name, P.Name, PI.Time, OT.Type ORDER BY Price ASC) AS row_num,
        ROUND(SUM(TotalQty) OVER (PARTITION BY E.Name, P.Name, PI.Time, OT.Type ORDER BY Price ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 3) AS CumulativeTotalQty,
        ROUND(SUM(TotalQty * Price) OVER (PARTITION BY E.Name, P.Name, PI.Time, OT.Type ORDER BY Price ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS CumulativeAmount,
        ROUND(AVG(Price) OVER (PARTITION BY E.Name, P.Name, PI.Time, OT.Type ORDER BY Price ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS AvgPrice,
        SUM(NumOrders) OVER (PARTITION BY E.Name, P.Name, PI.Time, OT.Type ORDER BY Price ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS CumulativeNumOrders
    FROM 
        (deltalive_gold.factOrderBook_gold) AS OB
    LEFT OUTER JOIN deltalive_gold.dimExchange_gold AS E ON E.Id = OB.ExchangeId
    LEFT OUTER JOIN deltalive_gold.dimProduct_gold AS P ON P.Id = OB.ProductId
    LEFT OUTER JOIN deltalive_gold.dimOrderType_gold AS OT ON OT.Id = OB.OrderTypeId
    LEFT OUTER JOIN deltalive_gold.dimPollInfo_gold AS PI ON PI.Id = OB.PollInfoId
    WHERE OT.Type = 'Ask'
    GROUP BY 
        E.Name, P.Name, PI.Time, OT.Type, OB.Price, OB.TotalQty, OB.NumOrders)
    WHERE row_num <= 1000

-- COMMAND ----------


