{{
    config(
        materialized='table'
    )
}}

SELECT 
    date::date AS case_date,
    states AS state_code,
    positive AS positive_cases,
    negative AS negative_cases,
    hospitalizedCurrently AS currently_hospitalized,
    death AS total_deaths
FROM 
    {{ source('raw', 'covid_data') }}
WHERE date IS NOT NULL and states IS NOT NULL