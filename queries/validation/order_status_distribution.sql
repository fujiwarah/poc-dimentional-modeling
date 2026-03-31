-- 注文ステータス分布の検証
-- 目的: 生成データの注文ステータス分布が設計通りか確認する
-- 期待される分布（仕様書 3.4）:
--   delivered 55% / shipped 12% / confirmed 10% / pending 8% / cancelled 15%

SELECT
  dos.status_code,
  COUNT(DISTINCT f.order_id) AS order_count,
  ROUND(COUNT(DISTINCT f.order_id) * 100.0 /
    NULLIF((SELECT COUNT(DISTINCT order_id) FROM dwh.fact_sales), 0), 1) AS pct
FROM dwh.fact_sales f
INNER JOIN dwh.dim_order_status dos ON f.order_status_key = dos.order_status_key
GROUP BY dos.status_code
ORDER BY order_count DESC
