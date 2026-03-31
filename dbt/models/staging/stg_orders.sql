SELECT
    CAST(order_id AS INT64) AS order_id,
    CAST(customer_id AS INT64) AS customer_id,
    CAST(order_date AS DATE) AS order_date,
    CAST(shipping_address AS STRING) AS shipping_address,
    LOWER(TRIM(CAST(status AS STRING))) AS status,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(updated_at AS TIMESTAMP) AS updated_at
FROM {{ source('raw', 'orders') }}
