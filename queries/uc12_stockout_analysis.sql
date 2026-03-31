-- UC-12: 欠品分析
-- 商品ごとの欠品日数と欠品率を把握し、在庫管理の改善に活用する

SELECT
  p.product_name,
  COUNT(CASE WHEN inv.is_out_of_stock THEN 1 END) AS stockout_days,
  ROUND(COUNT(CASE WHEN inv.is_out_of_stock THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 1)
    AS stockout_pct
FROM dwh.fact_inventory_daily inv
INNER JOIN dwh.dim_product p ON inv.product_key = p.product_key
GROUP BY p.product_name
HAVING COUNT(CASE WHEN inv.is_out_of_stock THEN 1 END) > 0
ORDER BY stockout_days DESC
