-- UC-2: 日次売上の7日移動平均
-- 日次売上のノイズを平滑化し、トレンドを把握する

SELECT
  d.full_date,
  SUM(f.net_amount) AS daily_revenue,
  AVG(SUM(f.net_amount)) OVER (
    ORDER BY d.full_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS moving_avg_7d
FROM dwh.fact_sales f
INNER JOIN dwh.dim_date d ON f.date_key = d.date_key
GROUP BY d.full_date
ORDER BY d.full_date
