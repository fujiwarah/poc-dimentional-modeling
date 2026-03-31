WITH order_items AS (
    SELECT * FROM {{ ref('int_orders_with_items') }}
    WHERE status != 'cancelled'
),

product_revenue AS (
    SELECT
        product_id,
        SUM(net_amount) AS total_revenue
    FROM order_items
    GROUP BY product_id
),

ranked AS (
    SELECT
        product_id,
        total_revenue,
        ROW_NUMBER() OVER (ORDER BY total_revenue DESC, product_id ASC) AS revenue_rank,
        SUM(total_revenue) OVER (ORDER BY total_revenue DESC, product_id ASC ROWS UNBOUNDED PRECEDING) AS cumulative_revenue
    FROM product_revenue
),

with_pct AS (
    SELECT
        r.*,
        SUM(total_revenue) OVER () AS grand_total
    FROM ranked r
)

SELECT
    product_id,
    total_revenue,
    revenue_rank,
    cumulative_revenue,
    cumulative_revenue / NULLIF(grand_total, 0) AS cumulative_pct,
    CASE
        WHEN cumulative_revenue / NULLIF(grand_total, 0) <= 0.6 THEN 'A'
        WHEN cumulative_revenue / NULLIF(grand_total, 0) <= 0.85 THEN 'B'
        ELSE 'C'
    END AS abc_rank
FROM with_pct
