{{ config(materialized='table') }}
WITH pivoted as(
    SELECT 
    see.country_id as country_id,
    sc.country_name as country_name,
    sc.region as region,
    see.year as year,
    sep.product_name AS energy_product,
    MAX(CASE WHEN see.activity_type = 'Production' THEN annualized_value END) AS production_volume,
    MAX(CASE WHEN see.activity_type = 'Consumption' THEN annualized_value END) AS consumption_volume,
    MAX(CASE WHEN see.activity_type = 'Imports' THEN annualized_value END) AS import_volume,
    MAX(CASE WHEN see.activity_type = 'Exports' THEN annualized_value END) AS export_volume,
    sep.annual_unit as volume_unit
    FROM {{ref('silver_eia_energy')}} see
    JOIN{{ref('silver_countries')}} sc 
    ON see.country_id = sc.country_id 
    JOIN {{ref('silver_energy_products')}} sep 
    ON see.energy_product_id = sep.energy_product_id
    GROUP BY see.country_id, sc.country_name, sc.region, see.year, sep.product_name, sep.annual_unit
),
calculated as (
    SELECT 
        *,
        CASE 
            WHEN production_volume IS NULL OR consumption_volume IS NULL THEN NULL
             WHEN production_volume / NULLIF(consumption_volume, 0) * 100 > 100 THEN 'Producer'
            ELSE 'Consumer'
        END as energy_role,
        CASE WHEN consumption_volume > 0 THEN import_volume/consumption_volume * 100 ELSE NULL END as import_dependency_pct,
        export_volume - import_volume as net_trade_balance
    FROM pivoted
),
metrics as (
    SELECT 
    *,
    CAST((import_volume - LAG(import_volume, 1) OVER (PARTITION BY country_id, energy_product ORDER BY year))
            / NULLIF( LAG(import_volume, 1) OVER (PARTITION BY country_id, energy_product ORDER BY year),0)
            * 100 AS DECIMAL(10,4)) AS yoy_import_change_pct,
    CAST((export_volume - LAG(export_volume, 1) OVER (PARTITION BY country_id, energy_product ORDER BY year))
            / NULLIF(LAG(export_volume, 1) OVER (PARTITION BY country_id, energy_product ORDER BY year),0)
            * 100 AS DECIMAL(10,4)) AS yoy_export_change_pct
    FROM calculated 

)

SELECT * FROM metrics 
