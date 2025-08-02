CREATE TABLE IF NOT EXISTS host_activity_reduced (
    host TEXT,
    month_start DATE,
    host_hits INT[],
    unique_visitors INT[],
    PRIMARY KEY (host, month_start)
);

INSERT INTO host_activity_reduced
WITH daily_aggregate AS (
    SELECT
        host,
        DATE(event_time) as date,
        COUNT(1) as num_host_hits,
        COUNT(DISTINCT user_id) as num_unique_visitors
    FROM events
    where DATE(event_time) = DATE('2023-01-03')
    and host is NOT NULL
    group BY host, DATE(event_time)
),

yesterday_array AS (
    SELECT *
    FROM host_activity_reduced
    WHERE month_start = DATE('2023-01-02')
)

SELECT 
    COALESCE(da.host, ya.host) as host,
    COALESCE(ya.month_start, DATE_TRUNC('month', da.date)) as month_start,
    CASE 
        WHEN ya.host_hits is not NULL
            THEN ya.host_hits || ARRAY[COALESCE(da.num_host_hits, 0)]
        WHEN ya.host_hits is NULL
            THEN ARRAY_FILL(0, ARRAY[COALESCE(da.date - DATE(DATE_TRUNC('month', da.date)), 0)]) || ARRAY[COALESCE(da.num_host_hits, 0)]
        WHEN ya.month_start is NULL
            THEN ARRAY[COALESCE(da.num_host_hits, 0)]
    END as host_hits,
    CASE 
        WHEN ya.unique_visitors IS NOT NULL
            THEN ya.unique_visitors || ARRAY[COALESCE(da.num_unique_visitors, 0)]
        WhEN ya.unique_visitors IS NULL
            THEN ARRAY_FILL(0, ARRAY[COALESCE(da.date - DATE(DATE_TRUNC('month', da.date)), 0)]) || ARRAY[COALESCE(da.num_unique_visitors, 0)]
        WHEN ya.month_start IS NULL
            THEN ARRAY[COALESCE(da.num_unique_visitors, 0)]
    END as unique_visitors
FROM daily_aggregate da
FULL OUTER JOIN yesterday_array ya
ON da.host = ya.host
ON CONFLICT (host, month_start) 
    DO UPDATE SET 
        host_hits = EXCLUDED.host_hits,
        unique_visitors = EXCLUDED.unique_visitors;
