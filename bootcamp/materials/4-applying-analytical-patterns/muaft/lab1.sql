--TRUNCATE users_growth_accounting;

INSERT INTO users_growth_accounting
with yesterday AS (
    SELECT * from users_growth_accounting
    WHERE date = DATE('2023-03-12')
),

today AS (
   SELECT 
    CAST(user_id as TEXT) as user_id,
    DATE_TRUNC('day', event_time::timestamp) as today_date,
    COUNT(1)
   FROM events
   WHERE user_id IS NOT NULL
   AND DATE_TRUNC('day', event_time::timestamp) = DATE('2023-03-13')
   group by user_id, date_trunc('day', event_time::timestamp)
) 

select 
    coalesce(t.user_id, y.user_id) as user_id,
    COALESCE(y.first_active_date, t.today_date) as first_active_date,
    COALESCE(t.today_date, y.last_active_date) as last_active_date,
    CASE 
        WHEN y.user_id IS NULL THEN 'New'
        WHEN y.last_active_date = t.today_date - INTERVAL '1 day' THEN 'Retained' 
        WHEN y.last_active_date < t.today_date - INTERVAL '1 day' THEN 'Resurrected' 
        WHEN t.today_date IS NULL and y.last_active_date = y.date THEN 'Churned'
        ELSE 'Stale'
    END as daily_active_state,
    CASE 
        WHEN y.user_id IS NULL THEN 'New'
        WHEN y.last_active_date >= t.today_date - INTERVAL '1 week' THEN 'Retained' 
        WHEN y.last_active_date < t.today_date - INTERVAL '1 week' THEN 'Resurrected' 
        WHEN t.today_date IS NULL AND y.last_active_date = t.today_date - INTERVAL '1 week' THEN 'Churned' 
        ELSE 'Stale'
    END as weekly_active_state,
    COALESCE(y.dates_active, ARRAY[]::DATE[]) || 
    CASE 
        WHEN t.user_id IS NOT NULL THEN ARRAY[t.today_date]
        ELSE ARRAY[]::DATE[]
    END AS dates_active,
    COALESCE(t.today_date, y.date + INTERVAL '1 day') as date
FROM today t
FULL OUTER JOIN yesterday y
on t.user_id = y.user_id;

select date, daily_active_state, count(1) from users_growth_accounting group by 1, 2;

select date - first_active_date as days_since_first_active , 
    COUNT(CASE WHEN daily_active_state in ('New', 'Retained', 'Resurrected') THEN 1 END) as number_active,
    CAST(COUNT(CASE WHEN daily_active_state in ('New', 'Retained', 'Resurrected') THEN 1 END) AS REAL) / COUNT(1),
    COUNT(1)
    from users_growth_accounting
    where first_active_date = DATE('2023-03-01')
GROUP BY date - first_active_date;