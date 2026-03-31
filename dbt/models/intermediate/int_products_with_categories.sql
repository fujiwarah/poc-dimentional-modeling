WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

categories AS (
    SELECT * FROM {{ ref('stg_categories') }}
)

SELECT
    p.product_id,
    p.product_name,
    c.category_name,
    parent.category_name AS parent_category_name,
    p.unit_price,
    p.cost_price,
    CASE
        WHEN p.cost_price IS NULL THEN NULL
        ELSE ROUND((p.unit_price - p.cost_price) / NULLIF(p.unit_price, 0) * 100, 2)
    END AS gross_margin_pct,
    p.is_active
FROM products p
LEFT JOIN categories c ON p.category_id = c.category_id
LEFT JOIN categories parent ON c.parent_id = parent.category_id
