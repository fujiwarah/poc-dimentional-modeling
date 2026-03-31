WITH products AS (
    SELECT * FROM {{ ref('int_products_with_categories') }}
),

abc AS (
    SELECT * FROM {{ ref('int_product_abc') }}
)

SELECT
    p.product_id AS product_key,
    p.product_id,
    p.product_name,
    p.category_name,
    p.parent_category_name,
    p.unit_price,
    p.cost_price,
    p.gross_margin_pct,
    p.is_active,
    COALESCE(a.abc_rank, 'C') AS abc_rank
FROM products p
LEFT JOIN abc a ON p.product_id = a.product_id
