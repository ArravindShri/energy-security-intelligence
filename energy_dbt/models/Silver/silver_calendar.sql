{{ config(materialized='table') }}

WITH nums AS (
    SELECT TOP (9497) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
    FROM silver_countries a, silver_countries b, silver_countries c, silver_countries d, silver_countries e
)
SELECT
    DATEADD(DAY, n, CAST('2000-01-01' AS DATE)) AS date_key,
    YEAR(DATEADD(DAY, n, CAST('2000-01-01' AS DATE))) AS year,
    DATEPART(QUARTER, DATEADD(DAY, n, CAST('2000-01-01' AS DATE))) AS quarter,
    CAST(CONCAT('Q', DATEPART(QUARTER, DATEADD(DAY, n, CAST('2000-01-01' AS DATE)))) AS VARCHAR(2)) AS quarter_name,
    MONTH(DATEADD(DAY, n, CAST('2000-01-01' AS DATE))) AS month_number,
    CAST(DATENAME(MONTH, DATEADD(DAY, n, CAST('2000-01-01' AS DATE))) AS VARCHAR(20)) AS month_name,
    DATEPART(WEEKDAY, DATEADD(DAY, n, CAST('2000-01-01' AS DATE))) AS day_of_week,
    CAST(DATENAME(WEEKDAY, DATEADD(DAY, n, CAST('2000-01-01' AS DATE))) AS VARCHAR(20)) AS day_name,
    CASE WHEN DATENAME(WEEKDAY, DATEADD(DAY, n, CAST('2000-01-01' AS DATE))) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END AS is_weekend,
    CAST(CONCAT(YEAR(DATEADD(DAY, n, CAST('2000-01-01' AS DATE))), '-', RIGHT('0' + CAST(MONTH(DATEADD(DAY, n, CAST('2000-01-01' AS DATE))) AS VARCHAR(2)), 2)) AS VARCHAR(7)) AS year_month
FROM nums