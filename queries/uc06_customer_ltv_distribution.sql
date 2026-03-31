-- UC-6: 顧客LTV分布
-- RFMセグメント・年代別にLTVの分布を把握し、高LTV顧客の特徴を分析する

SELECT
  c.rfm_segment,
  c.age_group,
  COUNT(*) AS customer_count,
  ROUND(AVG(c.ltv_estimated), 0) AS avg_ltv,
  ROUND(AVG(c.total_spent), 0) AS avg_total_spent,
  ROUND(AVG(c.total_orders), 1) AS avg_orders
FROM dwh.dim_customer c
WHERE c.total_orders > 0
GROUP BY c.rfm_segment, c.age_group
ORDER BY avg_ltv DESC
