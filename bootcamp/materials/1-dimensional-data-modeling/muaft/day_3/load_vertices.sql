insert into vertices
select 
    game_id as identifier,
    'game' as type,
    json_build_object(
        'pts_home', pts_home,
        'pts_away', pts_away,
        'winning_team', case when home_team_wins = 1 then home_team_id else visitor_team_id end
    )
from games;


insert into vertices
with players as (
    select 
        player_id as identifier,
        MAX(player_name) as player_name,
        count(1) as number_of_games,
        sum(pts) as total_points,
        ARRAY_AGG(DISTINCT(team_id)) as teams
    from game_details
    GROUP BY player_id
)

select 
    identifier,
    'player' as type,
    json_build_object(
        'player_name', player_name,
        'number_of_games', number_of_games,
        'total_points', total_points,
        'teams', teams
    )
from players;

insert into vertices
with teams_deduped as (
    SELECT *, 
    row_number() OVER(PARTITION BY team_id) as row_num 
    FROM teams
)
SELECT    
    team_id as identifier,
    'team' as type,
    json_build_object(
        'abbreviation', abbreviation,
        'nickname', nickname,
        'city', city,
        'arena', arena,
        'year_founded', yearfounded
    )
FROM teams_deduped
WHERE row_num = 1;

select type, count(1) 
from vertices
GROUP BY 1;
