with cumulated as (
    SELECT 
        user_id,
        browser_type,
        dates_active,
        date
    from user_devices_cumulated
    where date = DATE('2023-01-31')
),

series AS (
    select * from generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day') as series_date
),

place_holder_ints as (
    SELECT 
        CASE
            WHEN dates_active @> ARRAY [DATE(s.series_date)]
            THEN CAST(POW(2, 32 - (c.date - DATE(s.series_date))) AS BIGINT)
            ELSE 0
        END as placeholder_int_value,
        *
    from cumulated c
    CROSS JOIN series s
)

SELECT 
    user_id,
    browser_type,
    CAST(CAST(sum(placeholder_int_value) as BIGINT) as BIT(32)),
    BIT_COUNT(CAST(CAST(sum(placeholder_int_value) as BIGINT) as BIT(32))) as dim_is_monthly_active,
    BIT_COUNT(CAST('11111110000000000000000000000000' AS BIT(32)) 
        & CAST(CAST(sum(placeholder_int_value) as BIGINT) as BIT(32))) as dim_is_weekly_active
from place_holder_ints 
GROUP BY user_id, browser_type