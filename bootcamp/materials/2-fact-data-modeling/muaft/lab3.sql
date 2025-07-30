/*
CREATE TABLE array_metrics (
    user_id NUMERIC,
    month_start DATE,
    metric_name TEXT,
    metric_array REAL[],
    PRIMARY KEY (user_id, month_start, metric_name)
);
*/

INSERT INTO array_metrics
WITH daily_aggregate AS (
    select
    user_id,
    DATE(event_time) as date,
    COUNT(1) as num_site_hits
    FROM events
    where DATE(event_time) = DATE('2023-01-03')
    and user_id is NOT NULL
    group BY user_id, DATE(event_time)
),

yesterday_array as (
    SELECT * from array_metrics
    where month_start = DATE('2023-01-01')
)

SELECT 
COALESCE(da.user_id, ya.user_id) as user_id,
COALESCE(ya.month_start, DATE_TRUNC('month', da.date)) as month_start,
'site_hits' as metric_name,
CASE
    WHEN ya.metric_array is not NULL
    THEN ya.metric_array || ARRAY[COALESCE(da.num_site_hits, 0)]
    WHEN ya.metric_array is NULL
    THEN ARRAY_FILL(0, ARRAY[COALESCE(da.date - DATE(DATE_TRUNC('month', date)), 0)]) || ARRAY[COALESCE(da.num_site_hits, 0)]
    WHEN ya.month_start is NULL
    THEN ARRAY[COALESCE(da.num_site_hits, 0)]

END as t
from daily_aggregate da
FULL OUTER JOIN yesterday_array ya
on da.user_id = ya.user_id
ON CONFLICT (user_id, month_start, metric_name) 
    DO UPDATE SET metric_array = EXCLUDED.metric_array;

select * from array_metrics;
--TRUNCATE array_metrics;
