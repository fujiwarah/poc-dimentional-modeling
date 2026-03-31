-- RFMスコア: 閾値ベースのシンプルなスコアリング（BigQuery Emulator互換）
WITH order_items AS (
    SELECT * FROM {{ ref('int_orders_with_items') }}
    WHERE status != 'cancelled'
),

customer_metrics AS (
    SELECT
        customer_id,
        DATE_DIFF(CURRENT_DATE(), MAX(order_date), DAY) AS recency_days,
        COUNT(DISTINCT order_id) AS frequency,
        SUM(net_amount) AS monetary
    FROM order_items
    GROUP BY customer_id
),

scored AS (
    SELECT
        customer_id,
        recency_days,
        frequency,
        monetary,
        -- Recency: fewer days = better = higher score
        CASE
            WHEN recency_days <= 30 THEN 5
            WHEN recency_days <= 90 THEN 4
            WHEN recency_days <= 180 THEN 3
            WHEN recency_days <= 365 THEN 2
            ELSE 1
        END AS rfm_recency_score,
        -- Frequency: more orders = better = higher score
        CASE
            WHEN frequency >= 20 THEN 5
            WHEN frequency >= 10 THEN 4
            WHEN frequency >= 5 THEN 3
            WHEN frequency >= 2 THEN 2
            ELSE 1
        END AS rfm_frequency_score,
        -- Monetary: higher spend = better = higher score
        CASE
            WHEN monetary >= 500000 THEN 5
            WHEN monetary >= 200000 THEN 4
            WHEN monetary >= 100000 THEN 3
            WHEN monetary >= 30000 THEN 2
            ELSE 1
        END AS rfm_monetary_score
    FROM customer_metrics
)

SELECT
    customer_id,
    recency_days,
    frequency,
    monetary,
    rfm_recency_score,
    rfm_frequency_score,
    rfm_monetary_score,
    CASE
        WHEN rfm_recency_score >= 4 AND rfm_frequency_score >= 4 AND rfm_monetary_score >= 4 THEN 'チャンピオン'
        WHEN rfm_recency_score >= 3 AND rfm_frequency_score >= 3 AND rfm_monetary_score >= 3 THEN 'ロイヤル'
        WHEN rfm_recency_score >= 4 AND rfm_frequency_score <= 2 THEN '有望'
        WHEN rfm_recency_score <= 2 AND rfm_frequency_score >= 3 AND rfm_monetary_score >= 3 THEN 'リスクあり'
        WHEN rfm_recency_score <= 2 AND rfm_frequency_score <= 2 THEN '休眠'
        ELSE 'その他'
    END AS rfm_segment
FROM scored
