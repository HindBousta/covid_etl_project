{{
    config(
        materialized='table'
    )
}}

SELECT DISTINCT state_code
FROM {{ ref('stg_covid_cases') }}
WHERE state_code IS NOT NULL