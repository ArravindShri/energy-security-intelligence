{{ config(materialized='table') }}
SELECT 
    sc.country_id as country_id,
    CASE 
    WHEN LEN(bae.period) = 7 THEN CAST(CONCAT(bae.period, '-01') AS DATE)
    WHEN LEN(bae.period) = 4 THEN CAST(CONCAT(bae.period, '-01-01') AS DATE)
END AS period,
YEAR(CASE 
    WHEN LEN(bae.period) = 7 THEN CAST(CONCAT(bae.period, '-01') AS DATE)
    WHEN LEN(bae.period) = 4 THEN CAST(CONCAT(bae.period, '-01-01') AS DATE)
END) AS year,
MONTH(CASE 
    WHEN LEN(bae.period) = 7 THEN CAST(CONCAT(bae.period, '-01') AS DATE)
    WHEN LEN(bae.period) = 4 THEN CAST(CONCAT(bae.period, '-01-01') AS DATE)
END) AS month,
    sep.energy_product_id as energy_product_id,
    CASE 
    WHEN bae.activityName = 'Generation' THEN 'Production'
    ELSE bae.activityName
END AS activity_type,
    TRY_CAST(bae.value AS DECIMAL(18,4)) AS raw_value,
    bae.unit as raw_unit,
    TRY_CAST(bae.value AS DECIMAL(18,4)) * TRY_CAST(sep.annual_conversion_factor AS DECIMAL(18,4)) as annualized_value,
    sep.annual_unit as annual_unit,
    bae.dataFlagDescription as data_flag,
    CAST(CASE WHEN sc.country_id IS NOT NULL THEN 1 ELSE 0 END as INT) as is_valid
FROM {{source('bronze','bronze_eia_energy')}} bae 
JOIN {{ref('silver_countries')}}sc
ON bae.countryRegionId = sc.eia_code
JOIN {{ ref('silver_energy_products') }} sep
ON CAST(bae.productId AS INT) = sep.eia_product_id
WHERE bae.value IS NOT NULL AND bae.value != ''AND bae.unit = CAST(sep.eia_unit AS VARCHAR(20))



