{{ config(
    materialized='table',
    pre_hook="DROP TABLE IF EXISTS dbo.gold_crisis_analysis"
) }}
WITH crisis_prices AS (
    SELECT * FROM {{ ref('int_crisis_prices') }}
),
crisis_summary AS (
    SELECT
        crisis_id, crisis_name, is_ongoing, crisis_duration_days,start_date,
        ticker, company_name, country_id, asset_type, category,
        MIN(trade_date) AS pre_crisis_date,
        MAX(trade_date) AS post_crisis_date,
        MIN(close_price_usd) AS crisis_low,
        MAX(close_price_usd) AS crisis_high
    FROM crisis_prices
    WHERE close_price_usd IS NOT NULL
    GROUP BY crisis_id, crisis_name, is_ongoing, crisis_duration_days,start_date,
        ticker, company_name, country_id, asset_type, category
),
with_low_dates AS (
    SELECT 
        cp.crisis_id,
        cp.ticker,
        cp.trade_date AS crisis_low_date
    FROM crisis_prices cp
    JOIN crisis_summary cs 
        ON cp.crisis_id = cs.crisis_id 
        AND cp.ticker = cs.ticker 
        AND cp.close_price_usd = cs.crisis_low
),
with_prices as (
    SELECT
        cs.*,
        pre.close_price_usd as pre_crisis_price,
        post.close_price_usd as post_crisis_price
    FROM crisis_summary cs
    LEFT JOIN {{ ref('silver_stock_prices') }} pre
    ON pre.ticker = cs.ticker AND pre.trade_date = cs.pre_crisis_date
    LEFT JOIN {{ ref('silver_stock_prices') }} post
    ON post.ticker = cs.ticker AND post.trade_date = cs.post_crisis_date
),
with_recovery AS (
    SELECT
        cp.crisis_id,
        cp.ticker,
        MIN(cp.trade_date) AS recovery_date
    FROM crisis_prices cp
    JOIN with_low_dates wld ON cp.crisis_id = wld.crisis_id AND cp.ticker = wld.ticker
    JOIN with_prices wp ON cp.crisis_id = wp.crisis_id AND cp.ticker = wp.ticker
    WHERE cp.trade_date > wld.crisis_low_date
        AND cp.close_price_usd >= wp.pre_crisis_price
    GROUP BY cp.crisis_id, cp.ticker
),
final_calc AS (
    SELECT
        wp.*,
        sc.country_name as country_name,
        geo.energy_role as energy_role,
        (post_crisis_price - pre_crisis_price) / NULLIF(pre_crisis_price, 0) * 100 as crisis_return_pct,
        (crisis_low - pre_crisis_price) / NULLIF(pre_crisis_price, 0) * 100 as max_drawdown_pct,
        CASE WHEN post_crisis_price >= pre_crisis_price THEN 1 ELSE 0 END AS has_recovered,
        DATEDIFF(DAY, wld.crisis_low_date, wr.recovery_date) as recovery_days
    FROM with_prices wp
    LEFT JOIN with_low_dates wld 
        ON wp.crisis_id = wld.crisis_id AND wp.ticker = wld.ticker
    LEFT JOIN with_recovery wr 
        ON wp.crisis_id = wr.crisis_id AND wp.ticker = wr.ticker
    LEFT JOIN {{ ref('silver_countries') }} sc 
        ON wp.country_id = sc.country_id
    LEFT JOIN {{ ref('silver_stocks_reference') }} ssr2 
    ON wp.ticker = ssr2.ticker
    LEFT JOIN {{ ref('silver_energy_products') }} sep2 
    ON ssr2.primary_energy_product_id = sep2.energy_product_id
    LEFT JOIN {{ ref('gold_energy_overview') }} geo 
    ON wp.country_id = geo.country_id 
    AND YEAR(wp.start_date) = geo.year 
    AND sep2.product_name = geo.energy_product
)
SELECT * FROM final_calc