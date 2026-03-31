WITH products AS (
    SELECT * FROM {{ ref('int_products_with_categories') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY product_id) AS product_key,
    product_id,
    product_name,
    category_name,
    parent_category_name,
    unit_price
FROM products
