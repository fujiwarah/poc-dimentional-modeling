WITH order_items AS (
    SELECT * FROM {{ ref('int_orders_with_items') }}
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

dim_order_status AS (
    SELECT * FROM {{ ref('dim_order_status') }}
)

SELECT
    dc.customer_key,
    dp.product_key,
    dd.date_key,
    dos.order_status_key,
    oi.order_id,
    oi.order_item_id,
    oi.quantity,
    oi.unit_price,
    oi.discount_rate,
    oi.gross_amount,
    oi.discount_amount,
    oi.net_amount
FROM order_items oi
INNER JOIN dim_customer dc ON oi.customer_id = dc.customer_id
INNER JOIN dim_product dp ON oi.product_id = dp.product_id
INNER JOIN dim_date dd ON CAST(FORMAT_DATE('%Y%m%d', oi.order_date) AS INT64) = dd.date_key
INNER JOIN dim_order_status dos ON oi.status = dos.status_code
