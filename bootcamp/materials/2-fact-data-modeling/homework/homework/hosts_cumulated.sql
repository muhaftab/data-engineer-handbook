CREATE TABLE IF NOT EXISTS hosts_cumulated (
    host TEXT,
    host_activity_datelist DATE[],
    date DATE,
    PRIMARY KEY (host, date)
);

INSERT INTO hosts_cumulated
WITH yesterday AS (
    SELECT
        host,
        host_activity_datelist,
        date
    FROM hosts_cumulated
    WHERE date = DATE('2023-01-01')
),

today AS (
    SELECT
        host,
        DATE(CAST(event_time as TIMESTAMP)) as date
    FROM events
    WHERE DATE(CAST(event_time as TIMESTAMP)) = DATE('2023-01-02') 
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
ON t.host = y.host
