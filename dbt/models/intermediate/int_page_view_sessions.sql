WITH page_views AS (
    SELECT * FROM {{ ref('stg_page_views') }}
)

SELECT
    session_id,
    MAX(customer_id) AS customer_id,
    MIN(referrer_type) AS referrer_type,
    MIN(view_timestamp) AS session_start,
    MAX(view_timestamp) AS session_end,
    COUNT(*) AS page_view_count,
    SUM(duration_seconds) AS total_duration_seconds,
    MAX(CASE WHEN page_type = 'cart' THEN TRUE ELSE FALSE END) AS has_cart,
    MAX(CASE WHEN page_type = 'complete' THEN TRUE ELSE FALSE END) AS has_purchase
FROM page_views
GROUP BY session_id
