DO $$
DECLARE 
    start_date DATE := '2023-01-01';
    end_date DATE := '2023-01-31';
    curr_date DATE := start_date;
BEGIN
    WHILE curr_date < end_date
    LOOP
        --RAISE NOTICE 'Current date: %', curr_date;
        INSERT INTO user_devices_cumulated
        WITH yesterday AS (
            SELECT *
            FROM user_devices_cumulated
            where date = curr_date
        ),

        today AS (
            SELECT 
                CAST(e.user_id AS TEXT) as user_id, 
                CASE 
                    WHEN lower(d.browser_type) like '%chrome%' then 'Chrome'
                    WHEN lower(d.browser_type) like '%safari%' then 'Safari'
                    WHEN lower(d.browser_type) like '%opera%' then 'Opera'
                    WHEN lower(d.browser_type) like '%edge%' then 'Edge'
                    WHEN lower(d.browser_type) like '%ie%' then 'Internet Exploer'
                    WHEN lower(d.browser_type) like '%firefox%' then 'Firefox'
                    ELSE 'Other'
                END as browser_type,
                DATE(CAST(e.event_time as TIMESTAMP)) as date
            FROM events e
            JOIN devices d
            ON d.device_id = d.device_id
            WHERE e.user_id > 0
            AND DATE(CAST(e.event_time as TIMESTAMP)) = curr_date + INTERVAL '1 day'
            GROUP BY 1, 2, 3
        )

        SELECT 
            COALESCE(t.user_id, y.user_id) as user_id,
            COALESCE(t.browser_type, y.browser_type) as browser_type,
            CASE
                WHEN y.dates_active IS NULL THEN ARRAY[t.date]  
                WHEN t.date IS NULL THEN y.dates_active
                ELSE y.dates_active || ARRAY[t.date]
            END as dates_active,
            COALESCE(t.date, y.date + INTERVAL '1 day') as date
        FROM today t
        FULL OUTER JOIN yesterday y
        ON t.user_id = y.user_id
        AND t.browser_type = y.browser_type;
        curr_date := curr_date + INTERVAL '1 day';
    END LOOP;
END $$;

--select * from user_devices_cumulated;