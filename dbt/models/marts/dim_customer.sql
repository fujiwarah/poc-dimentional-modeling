WITH customers AS (
    SELECT * FROM {{ ref('int_customers_with_region') }}
),

rfm AS (
    SELECT * FROM {{ ref('int_customer_rfm') }}
),

ltv AS (
    SELECT * FROM {{ ref('int_customer_ltv') }}
)

SELECT
    c.customer_id AS customer_key,
    c.customer_id,
    c.full_name,
    c.email,
    c.phone,
    c.city,
    c.prefecture,
    c.region,
    c.gender,
    c.age_group,
    c.registration_channel,
    DATE_DIFF(CURRENT_DATE(), CAST(c.created_at AS DATE), DAY) AS customer_tenure_days,
    r.rfm_recency_score,
    r.rfm_frequency_score,
    r.rfm_monetary_score,
    COALESCE(r.rfm_segment, '未購入') AS rfm_segment,
    l.first_order_date,
    COALESCE(l.total_orders, 0) AS total_orders,
    COALESCE(l.total_spent, 0) AS total_spent,
    l.ltv_estimated
FROM customers c
LEFT JOIN rfm r ON c.customer_id = r.customer_id
LEFT JOIN ltv l ON c.customer_id = l.customer_id
