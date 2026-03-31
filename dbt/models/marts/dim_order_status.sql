WITH statuses AS (
    SELECT DISTINCT status
    FROM {{ ref('stg_orders') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY status) AS order_status_key,
    status AS status_code,
    CASE status
        WHEN 'pending' THEN '保留中'
        WHEN 'confirmed' THEN '確認済'
        WHEN 'shipped' THEN '発送済'
        WHEN 'delivered' THEN '配達完了'
        WHEN 'cancelled' THEN 'キャンセル'
        ELSE '不明'
    END AS status_label
FROM statuses
