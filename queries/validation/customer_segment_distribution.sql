-- 顧客セグメント分布の検証
-- 目的: 生成データのRFMセグメント分布と注文数ベースの分布が設計通りか確認する
-- 期待される分布（仕様書 3.2 セグメント分布）:
--   ヘビーユーザー 5% / ミドルユーザー 15% / ライトユーザー 30%
--   休眠ユーザー 20% / 新規ユーザー 20% / 離脱ユーザー 10%

-- RFMセグメント別の分布
SELECT
  'RFMセグメント分布' AS analysis_type,
  rfm_segment,
  COUNT(*) AS customer_count,
  ROUND(COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER (), 0), 1) AS pct
FROM dwh.dim_customer
GROUP BY rfm_segment
ORDER BY customer_count DESC
