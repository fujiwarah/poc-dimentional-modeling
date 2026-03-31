WITH date_range AS (
    SELECT
        MIN(order_date) AS min_date,
        MAX(order_date) AS max_date
    FROM {{ ref('stg_orders') }}
),

date_spine AS (
    SELECT
        DATE_ADD(dr.min_date, INTERVAL seq DAY) AS full_date
    FROM date_range dr
    CROSS JOIN UNNEST(
        GENERATE_ARRAY(0, DATE_DIFF(dr.max_date, dr.min_date, DAY))
    ) AS seq
)

SELECT
    CAST(FORMAT_DATE('%Y%m%d', d.full_date) AS INT64) AS date_key,
    d.full_date,
    EXTRACT(YEAR FROM d.full_date) AS year,
    EXTRACT(QUARTER FROM d.full_date) AS quarter,
    EXTRACT(MONTH FROM d.full_date) AS month,
    FORMAT_DATE('%B', d.full_date) AS month_name,
    MOD(EXTRACT(DAYOFWEEK FROM d.full_date) + 5, 7) AS day_of_week,
    FORMAT_DATE('%A', d.full_date) AS day_name,
    CASE
        WHEN EXTRACT(DAYOFWEEK FROM d.full_date) IN (1, 7) THEN TRUE
        ELSE FALSE
    END AS is_weekend
FROM date_spine d
ORDER BY d.full_date
