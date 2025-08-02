DO $$
DECLARE
    start_date DATE := '2023-01-01';
    end_date DATE := '2023-01-31';
    curr_date DATE := start_date;
BEGIN
    WHILE curr_date < end_date 
    LOOP
        INSERT INTO host_activity_reduced 
        WITH daily_aggregate AS (
            SELECT
                host,
                DATE(event_time) as date,
                COUNT(1) as num_host_hits,
                COUNT(DISTINCT user_id) as num_unique_visitors
            FROM events
            where DATE(event_time) = curr_date
            and host is NOT NULL
            group BY host, DATE(event_time)
        ),

        yesterday_array AS (
            SELECT *
            FROM host_activity_reduced
            WHERE month_start = DATE('2023-01-01')
        )

        SELECT 
            COALESCE(da.host, ya.host) as host,
            COALESCE(DATE(DATE_TRUNC('month', da.date)), ya.month_start) as month_start,
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
        curr_date := curr_date + INTERVAL '1 day';
    END LOOP;
END $$;
