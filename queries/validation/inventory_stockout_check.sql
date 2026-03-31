-- 欠品期間の存在確認
-- 目的: 生成データに意図的な欠品期間が含まれていることを確認する
-- 期待: 一部商品に欠品日数 > 0 が存在する（仕様書 3.7 在庫スナップショット）

SELECT
  p.product_name,
  COUNT(*) AS total_days,
  COUNT(CASE WHEN inv.is_out_of_stock THEN 1 END) AS stockout_days,
  ROUND(COUNT(CASE WHEN inv.is_out_of_stock THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS stockout_pct,
  MIN(CASE WHEN inv.is_out_of_stock THEN d.full_date END) AS first_stockout_date,
  MAX(CASE WHEN inv.is_out_of_stock THEN d.full_date END) AS last_stockout_date
FROM dwh.fact_inventory_daily inv
INNER JOIN dwh.dim_product p ON inv.product_key = p.product_key
INNER JOIN dwh.dim_date d ON inv.date_key = d.date_key
GROUP BY p.product_name
HAVING COUNT(CASE WHEN inv.is_out_of_stock THEN 1 END) > 0
ORDER BY stockout_days DESC
