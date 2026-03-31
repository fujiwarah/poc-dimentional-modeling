-- 月次売上の季節性検証
-- 目的: 月次売上に季節性パターン（12月・3月にピーク等）があることを確認する
-- 期待される傾向（仕様書 3.3 季節性）:
--   電化製品: 3月（決算）、12月（年末商戦）にピーク
--   衣料品: 4月、10月、12月にピーク
--   食品: 2月、3月、12月にピーク

SELECT
  d.month,
  d.year,
  SUM(f.net_amount) AS monthly_revenue,
  COUNT(DISTINCT f.order_id) AS order_count
FROM dwh.fact_sales f
INNER JOIN dwh.dim_date d ON f.date_key = d.date_key
GROUP BY d.month, d.year
ORDER BY d.year, d.month
