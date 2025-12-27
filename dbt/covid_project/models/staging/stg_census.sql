{{ config(materialized='table') }}

SELECT
    lpad(state_fips::text, 2, '0') as state_fips,
    state_name,
    total_population::numeric,
    median_age::numeric,
    ingestion_date::date
FROM {{ source('raw', 'census_data') }}
WHERE state_fips IS NOT NULL