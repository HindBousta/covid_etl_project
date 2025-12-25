{{
    config(
        materialized='table'
    )
}}

WITH dates AS (
    SELECT DISTINCT
        case_date
    FROM {{ ref('stg_covid_cases') }}
)
SELECT
    case_date AS date_key,
    EXTRACT(YEAR FROM case_date) AS year,
    EXTRACT(MONTH FROM case_date) AS month,
    EXTRACT(DAY FROM case_date) AS day,
    TO_CHAR(case_date, 'Day') AS day_name
FROM dates
