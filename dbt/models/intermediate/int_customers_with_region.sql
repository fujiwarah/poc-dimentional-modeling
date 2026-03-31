WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
)

SELECT
    customer_id,
    CONCAT(last_name, ' ', first_name) AS full_name,
    email,
    phone,
    city,
    prefecture,
    CASE
        WHEN prefecture = '北海道' THEN '北海道'
        WHEN prefecture IN ('青森県', '岩手県', '宮城県', '秋田県', '山形県', '福島県') THEN '東北'
        WHEN prefecture IN ('茨城県', '栃木県', '群馬県', '埼玉県', '千葉県', '東京都', '神奈川県') THEN '関東'
        WHEN prefecture IN ('新潟県', '富山県', '石川県', '福井県', '山梨県', '長野県', '岐阜県', '静岡県', '愛知県') THEN '中部'
        WHEN prefecture IN ('三重県', '滋賀県', '京都府', '大阪府', '兵庫県', '奈良県', '和歌山県') THEN '関西'
        WHEN prefecture IN ('鳥取県', '島根県', '岡山県', '広島県', '山口県') THEN '中国'
        WHEN prefecture IN ('徳島県', '香川県', '愛媛県', '高知県') THEN '四国'
        WHEN prefecture IN ('福岡県', '佐賀県', '長崎県', '熊本県', '大分県', '宮崎県', '鹿児島県', '沖縄県') THEN '九州・沖縄'
        ELSE '不明'
    END AS region
FROM customers
