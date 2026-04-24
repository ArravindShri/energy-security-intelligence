{{ config(materialized='table') }}
SELECT 
    CAST(currency_pair as VARCHAR(50)) as currency_pair,
    CAST(trade_date as DATE)as trade_date,
    CAST(close_rate as DECIMAL(10,6)) as close_rate,
    CAST(
    (CAST(close_rate AS DECIMAL(10,6)) - LAG(CAST(close_rate AS DECIMAL(10,6))) OVER (PARTITION BY currency_pair ORDER BY trade_date))
    / LAG(CAST(close_rate AS DECIMAL(10,6))) OVER (PARTITION BY currency_pair ORDER BY trade_date)
    * 100
AS DECIMAL(10,6)) AS daily_change_pct,
    CAST(CASE WHEN currency_pair is not null and trade_date is not null and close_rate is not null THEN 1 ELSE 0 END AS INT) as is_valid
FROM {{ source('bronze', 'bronze_forex_rates') }}