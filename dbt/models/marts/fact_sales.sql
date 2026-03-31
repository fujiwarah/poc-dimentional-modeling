WITH order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
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
),

joined AS (
    SELECT
        dc.customer_key,
        dp.product_key,
        dd.date_key,
        dos.order_status_key,
        o.order_id,
        oi.order_item_id,
        oi.quantity,
        oi.unit_price,
        oi.discount_rate,
        oi.unit_price * oi.quantity AS gross_amount,
        (oi.unit_price * oi.quantity) * oi.discount_rate / 100 AS discount_amount,
        (oi.unit_price * oi.quantity) - ((oi.unit_price * oi.quantity) * oi.discount_rate / 100) AS net_amount
    FROM order_items oi
    INNER JOIN orders o ON oi.order_id = o.order_id
    INNER JOIN dim_customer dc ON o.customer_id = dc.customer_id
    INNER JOIN dim_product dp ON oi.product_id = dp.product_id
    INNER JOIN dim_date dd ON CAST(FORMAT_DATE('%Y%m%d', o.order_date) AS INT64) = dd.date_key
    INNER JOIN dim_order_status dos ON o.status = dos.status_code
)

SELECT
    customer_key,
    product_key,
    date_key,
    order_status_key,
    order_id,
    order_item_id,
    quantity,
    unit_price,
    discount_rate,
    gross_amount,
    discount_amount,
    net_amount
FROM joined
