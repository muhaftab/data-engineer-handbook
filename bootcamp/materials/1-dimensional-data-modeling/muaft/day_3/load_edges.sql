insert into edges
with deduped as (
    select *,
    row_number() OVER(PARTITION BY game_id, player_id) as row_num
    from game_details
),
filtered as (
    select * from deduped
    where row_num = 1
),
aggregated as (
    SELECT
        f1.player_id as subject_player_id,
        MAX(f1.player_name) as subject_player_name,
        f2.player_id as object_player_id,
        MAX(f2.player_name) as object_player_name,
        CASE
            WHEN f1.team_id = f2.team_id THEN 'shares_team'::edge_type
            ELSE 'plays_against'::edge_type
        END as edge_type,
        count(1) as number_of_games,
        sum(f1.pts) as subject_points,
        sum(f2.pts) as object_points
    FROM filtered f1
    JOIN filtered f2
    ON f1.game_id = f2.game_id
    AND f1.player_name <> f2.player_name
    WHERE f1.player_id < f2.player_id
    GROUP BY f1.player_id, f2.player_id, edge_type
)
select 
    subject_player_id as subject_identifier,
    'player'::vertex_type as subject_type,
    object_player_id as object_identifier,
    'player'::vertex_type as object_type,
    edge_type,
    json_build_object(
        'num_games', number_of_games,
        'subject_points', subject_points,
        'object_points', object_points
    ) as properties 
from aggregated



insert into edges
select 
    player_id as subject_identifier,
    'player' as subject_type,
    game_id as object_identifier,
    'game' as object_type,
    'plays_in' as edge_type,
    json_build_object(
        'start_position', start_position,
        'pts', pts,
        'team_id', team_id,
        'team_abbreviation', team_abbreviation
    )
 from filtered
