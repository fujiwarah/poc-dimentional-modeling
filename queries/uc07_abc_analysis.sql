-- UC-7: ABC分析
-- 商品をABC分析ランク別に集計し、売上・利益への貢献度を把握する

SELECT
  p.abc_rank,
  COUNT(DISTINCT p.product_key) AS product_count,
  SUM(f.net_amount) AS total_revenue,
  SUM(f.gross_profit) AS total_profit,
  ROUND(SUM(f.net_amount) / NULLIF((SELECT SUM(net_amount) FROM dwh.fact_sales), 0) * 100, 1)
    AS revenue_share_pct
FROM dwh.fact_sales f
INNER JOIN dwh.dim_product p ON f.product_key = p.product_key
GROUP BY p.abc_rank
ORDER BY p.abc_rank
