insert into players_scd
with with_previous as (
    select 
        player_name, 
        current_season,
        scoring_class, 
        is_active,
        lag(scoring_class, 1) over (PARTITION BY player_name ORDER BY current_season) as previous_scoring_class,
        lag(is_active, 1) over (PARTITION BY player_name ORDER BY current_season) as previous_is_active
    from players
),

with_indicators as (
    select *, 
    case 
        when scoring_class = previous_scoring_class then 0
        WHEN is_active = previous_is_active then 0
        else 1
    end as change_indicator
    from with_previous
),

with_streaks as (
    select *,
        sum(change_indicator) over (PARTITION BY player_name ORDER BY current_season) as streak_identifier
    from with_indicators
)

select 
    player_name,
    scoring_class,
    is_active,
    min(current_season) as start_season,
    max(current_season) as end_season,
    2021 as current_season
from with_streaks
group by player_name, streak_identifier, is_active, scoring_class
order by player_name, streak_identifier;

select * from players_scd;