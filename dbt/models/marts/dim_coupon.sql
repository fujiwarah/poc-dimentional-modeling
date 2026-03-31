WITH coupons AS (
    SELECT * FROM {{ ref('stg_coupons') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY coupon_id) AS coupon_key,
    coupon_code,
    coupon_type,
    discount_value,
    campaign_name
FROM coupons
