{{ config(materialized='table') }}

SELECT DISTINCT
    state_fips,
    state_name,
    total_population,
    median_age
FROM {{ ref('stg_census') }}
