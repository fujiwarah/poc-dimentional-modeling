-- UC-3: 時間帯別売上
-- 時間帯ごとの売上傾向を把握し、プロモーション施策に活用する

SELECT
  t.time_period,
  t.hour_of_day,
  COUNT(DISTINCT f.order_id) AS order_count,
  SUM(f.net_amount) AS total_revenue
FROM dwh.fact_sales f
INNER JOIN dwh.dim_time t ON f.time_key = t.time_key
GROUP BY t.time_period, t.hour_of_day
ORDER BY t.hour_of_day
