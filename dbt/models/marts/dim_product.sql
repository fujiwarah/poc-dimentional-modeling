WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

categories AS (
    SELECT * FROM {{ ref('stg_categories') }}
),

joined AS (
    SELECT
        p.product_id,
        p.product_name,
        c.category_name,
        parent.category_name AS parent_category_name,
        p.unit_price
    FROM products p
    LEFT JOIN categories c ON p.category_id = c.category_id
    LEFT JOIN categories parent ON c.parent_id = parent.category_id
)

SELECT
    ROW_NUMBER() OVER (ORDER BY product_id) AS product_key,
    product_id,
    product_name,
    category_name,
    parent_category_name,
    unit_price
FROM joined
