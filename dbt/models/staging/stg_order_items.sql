SELECT
    CAST(order_item_id AS INT64) AS order_item_id,
    CAST(order_id AS INT64) AS order_id,
    CAST(product_id AS INT64) AS product_id,
    CAST(quantity AS INT64) AS quantity,
    CAST(unit_price AS NUMERIC) AS unit_price,
    CAST(discount AS NUMERIC) AS discount_rate
FROM {{ source('raw', 'order_items') }}
