DO $$
DECLARE
    start_date DATE := '2023-01-01';
    end_date DATE := '2023-01-31';
    curr_date DATE := start_date;
BEGIN
    WHILE curr_date < end_date 
    LOOP
        INSERT INTO hosts_cumulated
        WITH yesterday AS (
            SELECT
                host,
                host_activity_datelist,
                date
            FROM hosts_cumulated
            WHERE date = curr_date
        ),

        today AS (
            SELECT
                host,
                DATE(CAST(event_time as TIMESTAMP)) as date
            FROM events
            WHERE DATE(CAST(event_time as TIMESTAMP)) = curr_date + INTERVAL '1 day'
            GROUP BY 1,2
        )

        SELECT 
            COALESCE(t.host, y.host) as host,
            CASE 
                WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.date]
                WHEN t.date IS NULL THEN y.host_activity_datelist
                ELSE y.host_activity_datelist || ARRAY[t.date]
            END as host_activity_datelist,
            COALESCE(t.date, y.date + INTERVAL '1 day') as date
        FROm today t
        FULL OUTER JOIN yesterday y
        ON t.host = y.host;
        curr_date := curr_date + INTERVAL '1 day';
    END LOOP;
END $$;

select * from hosts_cumulated order by date desc;

