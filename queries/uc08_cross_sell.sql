-- UC-8: クロスセル分析（同一注文内の同時購入商品ペア）
-- 同一注文内で同時購入される商品ペアを特定し、クロスセル施策に活用する

WITH order_products AS (
  SELECT DISTINCT
    f.order_id,
    p.product_name,
    p.parent_category_name
  FROM dwh.fact_sales f
  INNER JOIN dwh.dim_product p ON f.product_key = p.product_key
)
SELECT
  a.product_name AS product_a,
  b.product_name AS product_b,
  COUNT(*) AS co_purchase_count
FROM order_products a
INNER JOIN order_products b
  ON a.order_id = b.order_id
  AND a.product_name < b.product_name
GROUP BY a.product_name, b.product_name
HAVING COUNT(*) >= 10
ORDER BY co_purchase_count DESC
LIMIT 20
