-- UC-5: コホート分析（月次リテンション）
-- 初回購入月ごとのコホートが翌月以降にどの程度リテンションするかを把握する

WITH first_purchase AS (
  SELECT
    customer_key,
    MIN(d.year_month) AS cohort_month
  FROM dwh.fact_sales f
  INNER JOIN dwh.dim_date d ON f.date_key = d.date_key
  GROUP BY customer_key
),
monthly_activity AS (
  SELECT DISTINCT
    f.customer_key,
    d.year_month AS activity_month
  FROM dwh.fact_sales f
  INNER JOIN dwh.dim_date d ON f.date_key = d.date_key
)
SELECT
  fp.cohort_month,
  ma.activity_month,
  COUNT(DISTINCT ma.customer_key) AS active_customers
FROM first_purchase fp
INNER JOIN monthly_activity ma ON fp.customer_key = ma.customer_key
GROUP BY fp.cohort_month, ma.activity_month
ORDER BY fp.cohort_month, ma.activity_month
