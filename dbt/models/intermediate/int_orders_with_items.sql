WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
)

SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.status,
    oi.order_item_id,
    oi.product_id,
    oi.quantity,
    oi.unit_price,
    oi.discount_rate,
    oi.unit_price * oi.quantity AS gross_amount,
    (oi.unit_price * oi.quantity) * oi.discount_rate / 100 AS discount_amount,
    (oi.unit_price * oi.quantity) - ((oi.unit_price * oi.quantity) * oi.discount_rate / 100) AS net_amount,
    o.order_timestamp,
    o.payment_method,
    o.device_type,
    o.coupon_code
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.order_id
