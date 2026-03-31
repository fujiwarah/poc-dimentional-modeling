WITH page_views AS (
    SELECT * FROM {{ ref('stg_page_views') }}
),

dim_customer AS (
    SELECT * FROM {{ ref('dim_customer') }}
),

dim_product AS (
    SELECT * FROM {{ ref('dim_product') }}
),

dim_date AS (
    SELECT * FROM {{ ref('dim_date') }}
),

dim_time AS (
    SELECT * FROM {{ ref('dim_time') }}
)

SELECT
    pv.page_view_id,
    dc.customer_key,
    dp.product_key,
    dd.date_key,
    dt.time_key,
    pv.session_id,
    pv.page_type,
    pv.referrer_type,
    pv.duration_seconds
FROM page_views pv
LEFT JOIN dim_customer dc ON pv.customer_id = dc.customer_id
LEFT JOIN dim_product dp ON pv.product_id = dp.product_id
INNER JOIN dim_date dd ON CAST(FORMAT_DATE('%Y%m%d', CAST(pv.view_timestamp AS DATE)) AS INT64) = dd.date_key
INNER JOIN dim_time dt ON EXTRACT(HOUR FROM pv.view_timestamp) = dt.time_key
