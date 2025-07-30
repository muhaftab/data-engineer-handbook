--select  *
--from events;
--
--drop table if exists users_accumulated;
--create table users_accumulated (
--    user_id TEXT,
--    dates_active DATE[],
--    date DATE,
--    PRIMARY KEY (user_id, date)
--);

/*
insert into users_accumulated
with yesterday as (
    select * from users_accumulated
    where date = DATE('2023-01-30')
),
today as (
    SELECT
    CAST(user_id as TEXT) AS user_id,
    DATE(CAST(event_time as TIMESTAMP)) AS date_active
    from events
    where DATE(CAST(event_time as TIMESTAMP)) = DATE('2023-01-31')
    AND user_id is not NULL
    GROUP BY user_id, date_active
)
SELECT 
    COALESCE(t.user_id, y.user_id) AS user_id,
    CASE
        WHEN y.dates_active IS NULL THEN ARRAY[t.date_active]
        WHEN t.date_active IS NULL THEN y.dates_active
        ELSE ARRAY_APPEND(y.dates_active, t.date_active)
    END as date_active,
    COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
from today t
FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id;
*/

--select * from users_accumulated where date = DATE('2023-01-31')

with users as (
    SELECT * from users_accumulated
    where date = DATE('2023-01-31')
),
series AS (
    select * from generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day') as series_date
),
place_holder_ints as (
    SELECT 
        CASE
            WHEN dates_active @> ARRAY [DATE(s.series_date)]
            THEN CAST(POW(2, 32 - (u.date - DATE(s.series_date))) AS BIGINT)
            ELSE 0
        END as placeholder_int_value,
        *
    from users u
    CROSS JOIN series s
)
SELECT 
user_id,
sum(placeholder_int_value),
CAST(CAST(sum(placeholder_int_value) as BIGINT) as BIT(32)),
BIT_COUNT(CAST(CAST(sum(placeholder_int_value) as BIGINT) as BIT(32))) as dim_is_monthly_active,
BIT_COUNT(CAST('11111110000000000000000000000000' AS BIT(32)) 
& CAST(CAST(sum(placeholder_int_value) as BIGINT) as BIT(32))) as dim_is_weekly_active
from place_holder_ints 
GROUP BY user_id