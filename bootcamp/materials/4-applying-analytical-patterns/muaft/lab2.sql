--CREATE table events_dashboard AS
--with combined as (
--    SELECT 
--        coalesce(d.browser_type, 'N/A') as browser_type,
--        coalesce(d.os_type, 'N/A') as os_type,
--        e.*, 
--        CASE 
--            WHEN Referrer like '%zackwilson%' THEN 'On Site' 
--            WHEN Referrer like '%eczackly%' THEN 'On Site' 
--            WHEN Referrer like '%dataengineer.io%' THEN 'On Site' 
--            WHEN Referrer like '%t.co%' THEN 'Twitter' 
--            WHEN Referrer like '%t.co%' THEN 'Twitter' 
--            WHEN Referrer like '%linkedin%' THEN 'Linkedin' 
--            WHEN Referrer like '%instagram%' THEN 'Instagram' 
--            WHEN Referrer IS NULL then 'Direct'
--            ELSE 'Other'
--        END as referrer_mapped
--    from events e
--    join devices d on e.device_id = d.device_id
--)
--select 
--    coalesce(referrer_mapped, '(overall)') as referrer,
--    coalesce(os_type, '(overall)') os_type,
--    coalesce(browser_type, '(overall)') browser_type,
--    count(1) as number_of_site_hits,
--    count(case when url = '/signup' then 1 END) as  number_of_signup_visits,
--    count(case when url = '/contact' then 1 END) as  number_of_contact_visits,
--    count(case when url = '/login' then 1 END) as  number_of_login_visits,
--    CAST(count(case when url = '/signup' then 1 END) AS REAL) / COUNT(1) as  pct_visited_signup
--from combined
--group by CUBE(referrer_mapped, browser_type, os_type)
--group by grouping sets (
--    (referrer_mapped, browser_type, os_type),
--    (os_type),
--    (browser_type),
--    (referrer_mapped),
--    ()
--)
--HAVING COUNT(1) > 100
--ORDER BY CAST(count(case when url = '/signup' then 1 END) AS REAL) / COUNT(1) DESC


with combined as (
    SELECT 
        coalesce(d.browser_type, 'N/A') as browser_type,
        coalesce(d.os_type, 'N/A') as os_type,
        e.user_id,
        e.url as url, 
        cast(e.event_time as timestamp) as event_time,
        CASE 
            WHEN Referrer like '%zackwilson%' THEN 'On Site' 
            WHEN Referrer like '%eczackly%' THEN 'On Site' 
            WHEN Referrer like '%dataengineer.io%' THEN 'On Site' 
            WHEN Referrer like '%t.co%' THEN 'Twitter' 
            WHEN Referrer like '%t.co%' THEN 'Twitter' 
            WHEN Referrer like '%linkedin%' THEN 'Linkedin' 
            WHEN Referrer like '%instagram%' THEN 'Instagram' 
            WHEN Referrer IS NULL then 'Direct'
            ELSE 'Other'
        END as referrer_mapped
    from events e
    join devices d on e.device_id = d.device_id
),
aggregated as (
    select 
        c1.url as to_page, 
        c2.url as from_page,
        MIN(c1.event_time - c2.event_time) as duration
    from combined c1
    join combined c2 
        ON c1.user_id = c2.user_id
        AND DATE(c1.event_time) = DATE(c2.event_time) 
        AND c1.event_time > c2.event_time
    group by c1.user_id, c1.url, c2.url
)
select to_page, from_page, 
count(1) as num_users,
min(duration),
max(duration),
avg(duration)
from aggregated
group by to_page, from_page
having count(1) > 10
