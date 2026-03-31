WITH customers AS (
    SELECT * FROM {{ ref('int_customers_with_region') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_key,
    customer_id,
    full_name,
    email,
    phone,
    city,
    prefecture,
    region
FROM customers
