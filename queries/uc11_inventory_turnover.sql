-- UC-11: 在庫回転率
-- 商品ごとの在庫回転率を算出し、在庫効率の改善に活用する

SELECT
  p.product_name,
  p.parent_category_name,
  ROUND(AVG(inv.quantity_on_hand), 0) AS avg_inventory,
  SUM(f_qty.total_sold) AS total_sold,
  ROUND(SUM(f_qty.total_sold) / NULLIF(AVG(inv.quantity_on_hand), 0), 2) AS turnover_rate
FROM dwh.fact_inventory_daily inv
INNER JOIN dwh.dim_product p ON inv.product_key = p.product_key
LEFT JOIN (
  SELECT product_key, SUM(quantity) AS total_sold
  FROM dwh.fact_sales
  GROUP BY product_key
) f_qty ON p.product_key = f_qty.product_key
GROUP BY p.product_name, p.parent_category_name
ORDER BY turnover_rate DESC
