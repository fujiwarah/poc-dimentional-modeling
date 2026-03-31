WITH order_items AS (
    SELECT * FROM {{ ref('int_orders_with_items') }}
    WHERE status != 'cancelled'
),

customer_orders AS (
    SELECT
        customer_id,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(net_amount) AS total_spent
    FROM order_items
    GROUP BY customer_id
),

customer_metrics AS (
    SELECT
        customer_id,
        first_order_date,
        last_order_date,
        total_orders,
        total_spent,
        total_spent / NULLIF(total_orders, 0) AS avg_order_amount,
        CASE
            WHEN total_orders = 1 THEN 1.0
            ELSE CAST(total_orders AS FLOAT64) / NULLIF(DATE_DIFF(last_order_date, first_order_date, DAY) / 365.0, 0)
        END AS annual_frequency
    FROM customer_orders
)

SELECT
    customer_id,
    first_order_date,
    last_order_date,
    total_orders,
    total_spent,
    avg_order_amount,
    annual_frequency,
    avg_order_amount * annual_frequency * 3 AS ltv_estimated
FROM customer_metrics
