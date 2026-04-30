{{ config(
    materialized='table',
    pre_hook="DROP TABLE IF EXISTS dbo.int_crisis_prices"
) }}
WITH windows as (
    SELECT 
        sce.crisis_id as crisis_id,
        sce.crisis_name as crisis_name,
        sce.start_date as start_date,
        sce.end_date as end_date,
        sce.is_ongoing as is_ongoing,
        DATEDIFF(DAY,sce.start_date,sce.end_date) as crisis_duration_days,
        DATEADD(DAY,-30,start_date) as analysis_window_start,
        DATEADD(DAY, 30, COALESCE(end_date, CAST(GETDATE() AS DATE))) as analysis_window_end
    FROM {{ref('silver_crisis_events')}} sce 
),

crisis_prices AS (
    SELECT
        w.*,
        ssr.ticker,
        ssr.company_name,
        ssr.country_id,
        ssr.asset_type,
        ssr.category,
        ssp.trade_date,
        ssp.close_price_usd
    FROM windows w
    CROSS JOIN {{ ref('silver_stocks_reference') }} ssr
    LEFT JOIN {{ ref('silver_stock_prices') }} ssp
        ON ssp.ticker = ssr.ticker
        AND ssp.trade_date BETWEEN w.analysis_window_start AND w.analysis_window_end
)

SELECT * FROM crisis_prices