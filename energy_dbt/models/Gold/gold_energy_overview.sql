{{ config(materialized='table') }}
WITH pivoted AS (
    SELECT 
        see.country_id,
        sc.country_name,
        sc.region,
        see.year,
        sep.product_name AS energy_product,
        MAX(CASE WHEN see.activity_type = 'Production' THEN annualized_value END) AS production_volume,
        MAX(CASE WHEN see.activity_type = 'Consumption' THEN annualized_value END) AS consumption_volume,
        MAX(CASE WHEN see.activity_type = 'Imports' THEN annualized_value END) AS import_volume,
        MAX(CASE WHEN see.activity_type = 'Exports' THEN annualized_value END) AS export_volume,
        sep.annual_unit AS volume_unit,
        sep.benchmark_ticker,
        sep.volume_to_price_conversion
    FROM {{ ref('silver_eia_energy') }} see
    JOIN {{ ref('silver_countries') }} sc ON see.country_id = sc.country_id
    JOIN {{ ref('silver_energy_products') }} sep ON see.energy_product_id = sep.energy_product_id
    GROUP BY see.country_id, sc.country_name, sc.region, see.year, sep.product_name, sep.annual_unit, sep.benchmark_ticker, sep.volume_to_price_conversion
),
with_prices AS (
    SELECT 
        p.country_id, p.country_name, p.region, p.year, p.energy_product,
        p.production_volume, p.consumption_volume, p.import_volume, p.export_volume,
        p.volume_unit, p.benchmark_ticker, p.volume_to_price_conversion,
        swb.gdp_usd,
        swb.population,
        swb.gdp_data_source,
        swb.year AS gdp_year_used,
        AVG(ssp.close_price) AS benchmark_price_avg
    FROM pivoted p
    LEFT JOIN {{ ref('silver_world_bank') }} swb ON p.country_id = swb.country_id AND p.year = swb.year
    LEFT JOIN {{ ref('silver_stock_prices') }} ssp ON ssp.ticker = p.benchmark_ticker AND YEAR(ssp.trade_date) = p.year
    GROUP BY p.country_id, p.country_name, p.region, p.year, p.energy_product,
        p.production_volume, p.consumption_volume, p.import_volume, p.export_volume,
        p.volume_unit, p.benchmark_ticker, p.volume_to_price_conversion,
        swb.gdp_usd, swb.population, swb.gdp_data_source, swb.year
),
calculated AS (
    SELECT
        country_id, country_name, region, year, energy_product,
        production_volume, consumption_volume, import_volume, export_volume, volume_unit,
        production_volume/ NULLIF(consumption_volume,0)*100 as self_sufficiency_ratio,
        CASE 
            WHEN production_volume IS NULL OR consumption_volume IS NULL THEN NULL
            WHEN production_volume / NULLIF(consumption_volume, 0) * 100 > 100 THEN 'Producer'
            ELSE 'Consumer'
        END as energy_role,
        export_volume - import_volume as net_trade_position,
        benchmark_price_avg,
        import_volume*benchmark_price_avg*volume_to_price_conversion as estimated_import_cost_usd,
        gdp_usd, 
        gdp_year_used, 
        gdp_data_source,
        (import_volume * benchmark_price_avg * volume_to_price_conversion) / NULLIF(gdp_usd, 0) * 100 as energy_cost_burden_pct,
        population,
        consumption_volume / NULLIF(population, 0) AS per_capita_consumption
    FROM with_prices
)
SELECT * FROM calculated