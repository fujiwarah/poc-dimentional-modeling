-- ページビューのファネル整合性テスト
-- 検証内容: page_type = 'complete' のセッションには必ず page_type = 'checkout' も存在すること
-- （complete があるのに checkout がないセッションは不正）
-- 0行返却 = テスト合格

WITH complete_sessions AS (
  SELECT DISTINCT session_id
  FROM {{ ref('fact_page_views') }}
  WHERE page_type = 'complete'
),
checkout_sessions AS (
  SELECT DISTINCT session_id
  FROM {{ ref('fact_page_views') }}
  WHERE page_type = 'checkout'
)
SELECT
  cs.session_id
FROM complete_sessions cs
LEFT JOIN checkout_sessions ck ON cs.session_id = ck.session_id
WHERE ck.session_id IS NULL
