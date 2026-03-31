SELECT
    CAST(coupon_id AS INT64) AS coupon_id,
    CAST(coupon_code AS STRING) AS coupon_code,
    CAST(coupon_type AS STRING) AS coupon_type,
    CAST(discount_value AS NUMERIC) AS discount_value,
    CAST(min_order_amount AS NUMERIC) AS min_order_amount,
    CAST(valid_from AS DATE) AS valid_from,
    CAST(valid_to AS DATE) AS valid_to,
    CAST(campaign_name AS STRING) AS campaign_name
FROM {{ source('raw', 'coupons') }}
