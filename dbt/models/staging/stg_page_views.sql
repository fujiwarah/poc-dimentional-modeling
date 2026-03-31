SELECT
    CAST(page_view_id AS INT64) AS page_view_id,
    CAST(customer_id AS INT64) AS customer_id,
    CAST(session_id AS STRING) AS session_id,
    CAST(page_type AS STRING) AS page_type,
    CAST(product_id AS INT64) AS product_id,
    CAST(category_id AS INT64) AS category_id,
    CAST(referrer_type AS STRING) AS referrer_type,
    CAST(view_timestamp AS TIMESTAMP) AS view_timestamp,
    CAST(duration_seconds AS INT64) AS duration_seconds
FROM {{ source('raw', 'page_views') }}
