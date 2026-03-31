-- 決済方法・デバイス分布の検証
-- 目的: 生成データの決済方法分布とデバイス分布が設計通りか確認する
-- 期待される分布（仕様書 3.4）:
--   決済方法: credit_card 55% / bank_transfer 15% / convenience_store 15% / carrier 10% / cod 5%
--   デバイス: mobile 60% / desktop 30% / tablet 10%

-- 決済方法分布
SELECT
  '決済方法分布' AS analysis_type,
  payment_method,
  COUNT(DISTINCT order_id) AS order_count,
  ROUND(COUNT(DISTINCT order_id) * 100.0 /
    NULLIF((SELECT COUNT(DISTINCT order_id) FROM dwh.fact_sales), 0), 1) AS pct
FROM dwh.fact_sales
GROUP BY payment_method
ORDER BY order_count DESC;

-- デバイス分布
SELECT
  'デバイス分布' AS analysis_type,
  device_type,
  COUNT(DISTINCT order_id) AS order_count,
  ROUND(COUNT(DISTINCT order_id) * 100.0 /
    NULLIF((SELECT COUNT(DISTINCT order_id) FROM dwh.fact_sales), 0), 1) AS pct
FROM dwh.fact_sales
GROUP BY device_type
ORDER BY order_count DESC
