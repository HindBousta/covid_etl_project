{{ config(materialized='table') }}

SELECT
    state_fips,
    state_name,
    total_population::numeric,
    median_age::numeric,
    ingestion_date::date
FROM {{ source('raw', 'census_data') }}
WHERE state_fips IS NOT NULL