-- UC-4: RFMセグメント別売上
-- RFMセグメントごとの売上貢献度を把握し、セグメント戦略に活用する

SELECT
  c.rfm_segment,
  COUNT(DISTINCT c.customer_key) AS customer_count,
  SUM(f.net_amount) AS total_revenue,
  ROUND(AVG(f.net_amount), 0) AS avg_order_amount
FROM dwh.fact_sales f
INNER JOIN dwh.dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.rfm_segment
ORDER BY total_revenue DESC
