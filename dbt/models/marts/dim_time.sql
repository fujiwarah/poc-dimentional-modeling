WITH hours AS (
    SELECT hour_val
    FROM UNNEST(GENERATE_ARRAY(0, 23)) AS hour_val
)

SELECT
    hour_val AS time_key,
    hour_val AS hour_of_day,
    CASE
        WHEN hour_val BETWEEN 0 AND 4 THEN '深夜'
        WHEN hour_val BETWEEN 5 AND 6 THEN '早朝'
        WHEN hour_val BETWEEN 7 AND 9 THEN '朝'
        WHEN hour_val BETWEEN 10 AND 11 THEN '昼前'
        WHEN hour_val BETWEEN 12 AND 14 THEN '昼'
        WHEN hour_val BETWEEN 15 AND 17 THEN '午後'
        WHEN hour_val BETWEEN 18 AND 20 THEN '夕方'
        ELSE '夜'
    END AS time_period,
    CASE
        WHEN hour_val BETWEEN 9 AND 17 THEN TRUE
        ELSE FALSE
    END AS is_business_hours
FROM hours
ORDER BY hour_val
