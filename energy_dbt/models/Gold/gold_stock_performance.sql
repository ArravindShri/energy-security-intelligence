{{ config(materialized='table') }}
WITH latest_prices AS (
    SELECT
        ssp.ticker,
        ssp.trade_date,
        ssp.close_price,
        ssp.close_price_usd,
        ssp.volume,
        ssp.daily_return_pct,
        ROW_NUMBER() OVER (PARTITION BY ssp.ticker ORDER BY ssp.trade_date DESC) AS rn
    FROM {{ ref('silver_stock_prices') }} ssp
),

current_prices AS (
    SELECT
        ticker,
        trade_date AS latest_date,
        close_price AS current_price,
        close_price_usd AS current_price_usd
    FROM latest_prices
    WHERE rn = 1
),

year_ago_prices AS (
    SELECT
        ssp.ticker,
        ssp.close_price_usd AS year_ago_price,
        ROW_NUMBER() OVER (PARTITION BY ssp.ticker ORDER BY ssp.trade_date DESC) AS rn
    FROM {{ ref('silver_stock_prices') }} ssp
    JOIN current_prices cp ON ssp.ticker = cp.ticker
    WHERE ssp.trade_date <= DATEADD(YEAR, -1, cp.latest_date)
),
performance AS (
    SELECT
        ticker,
        MAX(close_price_usd) AS week_52_high,
        MIN(close_price_usd) AS week_52_low,
        STDEV(daily_return_pct) * SQRT(252) as volatility,
        AVG(volume) as avg_daily_volume
    FROM latest_prices
    WHERE rn <= 252
    GROUP BY ticker
),

final AS (
    SELECT
        ssr.ticker,
        ssr.company_name,
        ssr.country_id,
        sc.country_name,
        geo.energy_role,
        ssr.asset_type,
        ssr.category,
        ssr.currency,
        cp.current_price,
        cp.current_price_usd,
        (cp.current_price_usd - yap.year_ago_price) / NULLIF(yap.year_ago_price, 0) * 100 AS yoy_return_pct,
        p.week_52_high,
        p.week_52_low,
        p.volatility,
        ((cp.current_price_usd - yap.year_ago_price) / NULLIF(yap.year_ago_price, 0) * 100 - rfr.rate_pct) / NULLIF(p.volatility, 0) AS sharpe_ratio,
        rfr.rate_pct AS risk_free_rate_used,
        p.avg_daily_volume,
        cp.latest_date AS price_date
    FROM {{ ref('silver_stocks_reference') }} ssr
    LEFT JOIN current_prices cp ON ssr.ticker = cp.ticker
    LEFT JOIN year_ago_prices yap ON ssr.ticker = yap.ticker AND yap.rn = 1
    LEFT JOIN performance p ON ssr.ticker = p.ticker
    LEFT JOIN {{ ref('silver_countries') }} sc ON ssr.country_id = sc.country_id
    LEFT JOIN {{ ref('silver_risk_free_rate') }} rfr ON YEAR(cp.latest_date) = rfr.year
    LEFT JOIN {{ ref('silver_energy_products') }} sep 
    ON ssr.primary_energy_product_id = sep.energy_product_id
    LEFT JOIN {{ ref('gold_energy_overview') }} geo 
    ON ssr.country_id = geo.country_id 
    AND YEAR(cp.latest_date) = geo.year 
    AND sep.product_name = geo.energy_product
)
SELECT * FROM final