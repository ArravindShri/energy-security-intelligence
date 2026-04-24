{{ config(materialized='table') }}
SELECT 
    CAST(bsep.ticker as VARCHAR(50)) as ticker,
    CAST(bsep.trade_date as DATE)as trade_date,
    CAST(bsep.open_price as DECIMAL(10,6)) as open_price,
    CAST(bsep.high_price as DECIMAL(10,6)) as high_price,
    CAST(bsep.low_price as DECIMAL(10,6))as low_price,
    CAST(bsep.close_price as DECIMAL(10,6))as close_price,
    CAST(CASE
            WHEN ssr.currency='USD' THEN bsep.close_price
            WHEN ssr.currency='INR'THEN bsep.close_price/fx_usd.close_rate
            WHEN ssr.currency='EUR' THEN bsep.close_price *(fx_eur.close_rate/fx_usd.close_rate) 
        ELSE 0
    END as DECIMAL(10,6)
    ) as close_price_usd,
    CAST(bsep.volume as BIGINT) as volume,
    (CAST(bsep.close_price AS DECIMAL(10,6))-LAG(CAST(bsep.close_price as DECIMAL(10,6)))OVER(PARTITION BY bsep.ticker ORDER BY bsep.trade_date))/LAG(CAST(bsep.close_price as DECIMAL(10,6)))OVER(PARTITION BY bsep.ticker ORDER BY bsep.trade_date)*100 as daily_return_pct,
    CAST(ssr.asset_type as VARCHAR(50)) as asset_type,
    CAST(CASE WHEN bsep.ticker IS NOT NULL AND bsep.trade_date IS NOT NULL AND bsep.close_price IS NOT NULL THEN 1 ELSE 0 END as INT) as is_valid
FROM {{ source('bronze', 'bronze_stock_etf_prices') }} bsep
JOIN {{ ref('silver_stocks_reference') }} ssr
ON ssr.ticker = bsep.ticker
LEFT JOIN {{ ref('silver_forex_rates') }} fx_usd
    ON fx_usd.currency_pair = 'USD/INR' 
    AND fx_usd.trade_date = bsep.trade_date

LEFT JOIN {{ ref('silver_forex_rates') }} fx_eur
    ON fx_eur.currency_pair = 'EUR/INR' 
    AND fx_eur.trade_date = bsep.trade_date