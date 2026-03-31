-- UC-9: コンバージョンファネル
-- ページ種別ごとのセッション数を集計し、ファネルの各段階でのドロップオフを把握する

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
