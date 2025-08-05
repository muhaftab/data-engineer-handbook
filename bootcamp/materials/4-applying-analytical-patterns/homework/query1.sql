/*
- A query that does state change tracking for `players`
  - A player entering the league should be `New`
  - A player leaving the league should be `Retired`
  - A player staying in the league should be `Continued Playing`
  - A player that comes out of retirement should be `Returned from Retirement`
  - A player that stays out of the league should be `Stayed Retired`
*/

CREATE TYPE player_status AS ENUM (
    'New',
    'Retired',
    'Continued Playing',
    'Returned from Retirement',
    'Stayed Retired'
);

with with_previous as (
    select 
        player_name, 
        current_season,
        years_since_last_season,
        lag(years_since_last_season, 1) OVER(PARTITION BY player_name ORDER BY current_season) as prev_years_since_last_season
    from players
),

with_status AS (
    SELECT
        *,
        CASE
            WHEN prev_years_since_last_season is null and years_since_last_season = 0 then 'New'::player_status
            WHEN prev_years_since_last_season = 0 and years_since_last_season = 0 then 'Continued Playing'::player_status
            WHEN years_since_last_season = 1 then 'Retired'::player_status
            WHEN years_since_last_season > 1 then 'Stayed Retired'::player_status
            WHEN prev_years_since_last_season >= 1 and years_since_last_season = 0 then 'Returned from Retirement'::player_status
        END as player_status
    from with_previous
),

with_indicators as (
    select *, 
    case 
        WHEN years_since_last_season = prev_years_since_last_season then 0
        else 1
    end as change_indicator
    from with_status
),

with_streaks as (
    select *,
        sum(change_indicator) over (PARTITION BY player_name ORDER BY current_season) as streak_identifier
    from with_indicators
)

select 
    player_name,
    player_status,
    max(years_since_last_season),
    min(current_season) as start_season,
    max(current_season) as end_season,
    2021 as current_season
from with_streaks
where player_name = 'Michael Jordan'
group by player_name, streak_identifier, player_status
order by player_name, start_season;
