-- UC-1: 月次売上推移と前年同月比
-- 月次の売上推移を把握し、前年同月比で成長率を分析する

WITH monthly_sales AS (
  SELECT
    d.fiscal_year,
    d.year_month,
    d.month,
    SUM(f.net_amount) AS monthly_revenue,
    COUNT(DISTINCT f.order_id) AS order_count
  FROM dwh.fact_sales f
  INNER JOIN dwh.dim_date d ON f.date_key = d.date_key
  GROUP BY d.fiscal_year, d.year_month, d.month
)
SELECT
  curr.year_month,
  curr.monthly_revenue,
  curr.order_count,
  prev.monthly_revenue AS prev_year_revenue,
  ROUND((curr.monthly_revenue - prev.monthly_revenue) / NULLIF(prev.monthly_revenue, 0) * 100, 1)
    AS yoy_growth_pct
FROM monthly_sales curr
LEFT JOIN monthly_sales prev
  ON curr.month = prev.month
  AND curr.fiscal_year = prev.fiscal_year + 1
ORDER BY curr.year_month
