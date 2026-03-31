-- ページビューのファネル変換率検証
-- 目的: 生成データのファネル変換率が設計通りか確認する
-- 期待される変換率（仕様書 3.6）:
--   top/search → category: 60%
--   category → product: 40%
--   product → cart: 15%
--   cart → checkout: 60%
--   checkout → complete: 85%

WITH funnel AS (
  SELECT
    page_type,
    COUNT(DISTINCT session_id) AS sessions
  FROM dwh.fact_page_views
  GROUP BY page_type
)
SELECT
  page_type,
  sessions,
  ROUND(sessions * 100.0 /
    NULLIF((SELECT sessions FROM funnel WHERE page_type = 'top'), 0), 1) AS pct_of_top
FROM funnel
WHERE page_type IN ('top', 'category', 'product', 'cart', 'checkout', 'complete')
ORDER BY sessions DESC
