{{
    config(
        materialized='incremental',
        unique_key=['date_key','state_key']
    )
}}

SELECT 
    c.case_date AS date_key,
    c.state_code AS state_key,
    c.positive_cases,
    c.negative_cases,
    c.currently_hospitalized,
    c.total_deaths
FROM 
    {{ ref('stg_covid_cases') }} c

{% if is_incremental() %}
WHERE c.case_date >= (
  SELECT max(date_key) - interval '1 day'
  FROM {{ this }}
)
{% endif %}