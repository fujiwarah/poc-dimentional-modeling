-- 性別・年代分布の検証
-- 目的: 生成データの性別分布と年代分布が設計通りか確認する
-- 期待される分布（仕様書 3.2）:
--   性別: 男性48% / 女性50% / その他2%
--   年代: 20代15% / 30代30% / 40代25% / 50代20% / 60代以上10%

-- 性別分布
SELECT
  '性別分布' AS analysis_type,
  gender,
  COUNT(*) AS customer_count,
  ROUND(COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER (), 0), 1) AS pct
FROM dwh.dim_customer
GROUP BY gender
ORDER BY customer_count DESC;

-- 年代分布
SELECT
  '年代分布' AS analysis_type,
  age_group,
  COUNT(*) AS customer_count,
  ROUND(COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER (), 0), 1) AS pct
FROM dwh.dim_customer
GROUP BY age_group
ORDER BY age_group
