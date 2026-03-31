SELECT
    CAST(order_id AS INT64) AS order_id,
    CAST(customer_id AS INT64) AS customer_id,
    CAST(order_date AS DATE) AS order_date,
    CAST(shipping_address AS STRING) AS shipping_address,
    LOWER(TRIM(CAST(status AS STRING))) AS status,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(updated_at AS TIMESTAMP) AS updated_at,
    CAST(order_timestamp AS TIMESTAMP) AS order_timestamp,
    CAST(payment_method AS STRING) AS payment_method,
    CAST(device_type AS STRING) AS device_type,
    CAST(coupon_code AS STRING) AS coupon_code
FROM {{ source('raw', 'orders') }}
