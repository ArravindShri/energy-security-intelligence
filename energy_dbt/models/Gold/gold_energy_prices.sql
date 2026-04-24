{{ config(materialized='table') }}
WITH monthly as ( 
SELECT 
    CAST(sep.product_name as VARCHAR(50)) as energy_product,
    CAST(CONCAT(YEAR(trade_date), '-', RIGHT('0' + CAST(MONTH(trade_date) AS VARCHAR(2)), 2), '-01') AS DATE) as period,
    YEAR(ssp.trade_date) AS year,
    MONTH(ssp.trade_date) AS month,
    ssr.ticker as benchmark_ticker,
    AVG(ssp.close_price) as avg_monthly_price,
    MAX(ssp.close_price) as high_monthly_price,
    MIN(ssp.close_price) as low_monthly_price,
    sep.price_unit as price_unit
FROM {{ref('silver_stock_prices')}} ssp
JOIN {{ref('silver_stocks_reference')}} ssr
ON ssp.ticker = ssr.ticker
JOIN {{ref('silver_energy_products')}} sep
ON ssr.ticker = sep.benchmark_ticker
WHERE ssr.ticker IN ('CL', 'NG')
GROUP BY sep.product_name, YEAR(ssp.trade_date), MONTH(ssp.trade_date), ssr.ticker, sep.price_unit, CAST(CONCAT(YEAR(trade_date), '-', RIGHT('0' + CAST(MONTH(trade_date) AS VARCHAR(2)), 2), '-01') AS DATE)
) 

SELECT 
*,
CAST((avg_monthly_price - LAG(avg_monthly_price,1)OVER(Partition by benchmark_ticker ORDER BY year,month))/LAG(avg_monthly_price,1)OVER(Partition by benchmark_ticker ORDER BY year,month)*100 AS Decimal (10,6)) as price_mom_change_pct,
CAST((avg_monthly_price - LAG(avg_monthly_price,12)OVER(Partition by benchmark_ticker ORDER BY year,month))/LAG(avg_monthly_price,12)OVER(Partition by benchmark_ticker ORDER BY year,month)*100 AS Decimal (10,6)) as price_yoy_change_pct
FROM monthly
