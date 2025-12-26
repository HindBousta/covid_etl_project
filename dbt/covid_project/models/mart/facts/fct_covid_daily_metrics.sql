{{
    config(
        materialized='incremental',
        unique_key=['date_key','state_key']
    )
}}

WITH joined AS 
(SELECT 
    c.case_date AS date_key,
    c.state_code AS state_key,
    c.positive_cases,
    c.negative_cases,
    c.currently_hospitalized,
    c.total_deaths,
    s.total_population,
    s.median_age,
    s.state_name,
    (c.positive_cases / s.total_population) * 100000 AS cases_per_100k
FROM 
    {{ ref('stg_covid_cases') }} c
LEFT JOIN 
    {{ ref('stg_census') }} s
    ON c.state_code = s.state_fips
)
SELECT * FROM joined

{% if is_incremental() %}
WHERE c.case_date >= (
  SELECT max(date_key) - interval '1 day'
  FROM {{ this }}
)
{% endif %}