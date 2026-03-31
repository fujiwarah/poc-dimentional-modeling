-- UC-10: 商品閲覧→購入コンバージョン率
-- 商品ページを閲覧した商品が実際に購入されたかを把握し、コンバージョン改善に活用する

WITH viewed AS (
  SELECT DISTINCT product_key
  FROM dwh.fact_page_views
  WHERE page_type = 'product' AND product_key IS NOT NULL
),
purchased AS (
  SELECT DISTINCT product_key
  FROM dwh.fact_sales
)
SELECT
  p.product_name,
  CASE WHEN v.product_key IS NOT NULL THEN TRUE ELSE FALSE END AS was_viewed,
  CASE WHEN pu.product_key IS NOT NULL THEN TRUE ELSE FALSE END AS was_purchased
FROM dwh.dim_product p
LEFT JOIN viewed v ON p.product_key = v.product_key
LEFT JOIN purchased pu ON p.product_key = pu.product_key
