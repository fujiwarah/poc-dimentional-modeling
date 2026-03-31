WITH all_dates AS (
    SELECT MIN(order_date) AS min_date, MAX(order_date) AS max_date FROM {{ ref('stg_orders') }}
    UNION ALL
    SELECT CAST(MIN(view_timestamp) AS DATE), CAST(MAX(view_timestamp) AS DATE) FROM {{ ref('stg_page_views') }}
    UNION ALL
    SELECT MIN(snapshot_date), MAX(snapshot_date) FROM {{ ref('stg_inventory_snapshots') }}
),

date_range AS (
    SELECT
        MIN(min_date) AS min_date,
        MAX(max_date) AS max_date
    FROM all_dates
),

date_spine AS (
    SELECT
        DATE_ADD(dr.min_date, INTERVAL seq DAY) AS full_date
    FROM date_range dr
    CROSS JOIN UNNEST(
        GENERATE_ARRAY(0, DATE_DIFF(dr.max_date, dr.min_date, DAY))
    ) AS seq
)

SELECT
    CAST(FORMAT_DATE('%Y%m%d', d.full_date) AS INT64) AS date_key,
    d.full_date,
    EXTRACT(YEAR FROM d.full_date) AS year,
    EXTRACT(QUARTER FROM d.full_date) AS quarter,
    EXTRACT(MONTH FROM d.full_date) AS month,
    FORMAT_DATE('%B', d.full_date) AS month_name,
    MOD(EXTRACT(DAYOFWEEK FROM d.full_date) + 5, 7) AS day_of_week,
    FORMAT_DATE('%A', d.full_date) AS day_name,
    CASE
        WHEN EXTRACT(DAYOFWEEK FROM d.full_date) IN (1, 7) THEN TRUE
        ELSE FALSE
    END AS is_weekend,
    EXTRACT(ISOWEEK FROM d.full_date) AS week_of_year,
    FORMAT_DATE('%Y-%m', d.full_date) AS year_month,
    CONCAT(CAST(EXTRACT(YEAR FROM d.full_date) AS STRING), '-Q', CAST(EXTRACT(QUARTER FROM d.full_date) AS STRING)) AS year_quarter,
    CASE
        WHEN d.full_date IN (
            -- 2024年の祝日
            DATE '2024-01-01',  -- 元日
            DATE '2024-01-08',  -- 成人の日
            DATE '2024-02-11',  -- 建国記念の日
            DATE '2024-02-12',  -- 振替休日
            DATE '2024-02-23',  -- 天皇誕生日
            DATE '2024-03-20',  -- 春分の日
            DATE '2024-04-29',  -- 昭和の日
            DATE '2024-05-03',  -- 憲法記念日
            DATE '2024-05-04',  -- みどりの日
            DATE '2024-05-05',  -- こどもの日
            DATE '2024-05-06',  -- 振替休日
            DATE '2024-07-15',  -- 海の日
            DATE '2024-08-11',  -- 山の日
            DATE '2024-08-12',  -- 振替休日
            DATE '2024-09-16',  -- 敬老の日
            DATE '2024-09-22',  -- 秋分の日
            DATE '2024-09-23',  -- 振替休日
            DATE '2024-10-14',  -- スポーツの日
            DATE '2024-11-03',  -- 文化の日
            DATE '2024-11-04',  -- 振替休日
            DATE '2024-11-23',  -- 勤労感謝の日
            -- 2025年の祝日
            DATE '2025-01-01',  -- 元日
            DATE '2025-01-13',  -- 成人の日
            DATE '2025-02-11',  -- 建国記念の日
            DATE '2025-02-23',  -- 天皇誕生日
            DATE '2025-02-24',  -- 振替休日
            DATE '2025-03-20',  -- 春分の日
            DATE '2025-04-29',  -- 昭和の日
            DATE '2025-05-03',  -- 憲法記念日
            DATE '2025-05-04',  -- みどりの日
            DATE '2025-05-05',  -- こどもの日
            DATE '2025-05-06',  -- 振替休日
            DATE '2025-07-21',  -- 海の日
            DATE '2025-08-11',  -- 山の日
            DATE '2025-09-15',  -- 敬老の日
            DATE '2025-09-23',  -- 秋分の日
            DATE '2025-10-13',  -- スポーツの日
            DATE '2025-11-03',  -- 文化の日
            DATE '2025-11-23',  -- 勤労感謝の日
            DATE '2025-11-24',  -- 振替休日
            -- 2026年の祝日
            DATE '2026-01-01',  -- 元日
            DATE '2026-01-12',  -- 成人の日
            DATE '2026-02-11',  -- 建国記念の日
            DATE '2026-02-23',  -- 天皇誕生日
            DATE '2026-03-20',  -- 春分の日
            DATE '2026-04-29',  -- 昭和の日
            DATE '2026-05-03',  -- 憲法記念日
            DATE '2026-05-04',  -- みどりの日
            DATE '2026-05-05',  -- こどもの日
            DATE '2026-05-06',  -- 振替休日
            DATE '2026-07-20',  -- 海の日
            DATE '2026-08-11',  -- 山の日
            DATE '2026-09-21',  -- 敬老の日
            DATE '2026-09-22',  -- 国民の休日
            DATE '2026-09-23',  -- 秋分の日
            DATE '2026-10-12',  -- スポーツの日
            DATE '2026-11-03',  -- 文化の日
            DATE '2026-11-23'   -- 勤労感謝の日
        ) THEN TRUE
        ELSE FALSE
    END AS is_holiday,
    CASE
        WHEN EXTRACT(MONTH FROM d.full_date) >= 4 THEN EXTRACT(YEAR FROM d.full_date)
        ELSE EXTRACT(YEAR FROM d.full_date) - 1
    END AS fiscal_year,
    CASE
        WHEN EXTRACT(MONTH FROM d.full_date) BETWEEN 4 AND 6 THEN 1
        WHEN EXTRACT(MONTH FROM d.full_date) BETWEEN 7 AND 9 THEN 2
        WHEN EXTRACT(MONTH FROM d.full_date) BETWEEN 10 AND 12 THEN 3
        ELSE 4
    END AS fiscal_quarter
FROM date_spine d
ORDER BY d.full_date
