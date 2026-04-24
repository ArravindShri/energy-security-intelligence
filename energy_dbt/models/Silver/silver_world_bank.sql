{{ config(materialized='table') }}
SELECT 
    sc.country_id as country_id,
    CAST(bwbi.year as INT ) as year,
    CAST(MAX(CASE WHEN bwbi.indicator_code = 'NY.GDP.MKTP.CD' THEN value END) as DECIMAL(20,4)) AS gdp_usd,
    CAST(MAX(CASE WHEN bwbi.indicator_code = 'SP.POP.TOTL' THEN value END) as BIGINT) AS population,
    MAX(CASE WHEN bwbi.indicator_code = 'NY.GDP.MKTP.CD' THEN bwbi.data_source END) AS gdp_data_source,
    CAST(CASE WHEN sc.country_id IS NOT NULL THEN 1 ELSE 0 END as INT) as is_valid
FROM {{source('bronze','bronze_world_bank_indicators')}} bwbi 
JOIN {{ref('silver_countries')}} sc
ON bwbi.country_code = sc.eia_code
GROUP BY sc.country_id, CAST(bwbi.year as INT)
