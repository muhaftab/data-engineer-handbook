CREATE TABLE fct_game_details (
    dim_game_date DATE,
    dim_season INT,
    dim_team_id INT,
    dim_player_id INT,
    dim_player_name TEXT,
    dim_is_playing_at_home BOOLEAN,
    dim_start_position TEXT,
    dim_did_not_play BOOLEAN,
    dim_did_not_dress BOOLEAN,
    dim_not_with_team BOOLEAN,
    m_minutes REAL,
    m_fgm INT,
    m_fga INT,
    m_fg3m INT,
    m_ftm INT,
    m_fta INT,
    m_oreb INT,
    m_dreb INT,
    m_reb INT,
    m_ast INT,
    m_stl INT,
    m_blk INT,
    m_turnovers INT,
    m_pf INT,
    m_pts INT,
    m_plus_minus REAL,
    PRIMARY KEY (dim_game_date, dim_team_id, dim_player_id) 
);

insert into fct_game_details 
with deduped as (
    select
        g.game_date_est,
        g.season,
        g.home_team_id,
        gd.*,
        row_number() over(PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) as row_num
    from game_details gd
    join games g ON g.game_id = gd.game_id
)
select 
    game_date_est as dim_game_date, 
    season as dim_season,
    team_id as dim_team_id, 
    player_id as dim_player_id,
    player_name as dim_player_name,
    team_id = home_team_id as dim_is_playing_at_home,
    start_position as dim_start_position,
    COALESCE(POSITION('DNP' IN comment), 0) > 0 as dim_did_not_play,
    COALESCE(POSITION('DND' IN comment), 0) > 0 as dim_did_not_dress,
    COALESCE(POSITION('NWT' IN comment), 0) > 0 as dim_not_with_team,
    CAST(SPLIT_PART(min, ':', 1) AS REAL) +
    CAST(SPLIT_PART(min, ':', 2) AS REAL)/60 as m_minutes,
    fgm as m_fgm,
    fga as m_fga,
    fg3m as m_fg3m,
    ftm as m_ftm,
    fta as m_fta,
    oreb as m_oreb,
    dreb as m_dreb,
    reb as m_reb,
    ast as m_ast,
    stl as m_stl,
    blk as m_blk,
    "TO" as m_turnovers,
    pf as m_pf,
    pts as m_pts,
    plus_minus as m_plus_minus
from deduped
where row_num = 1;

select 
    dim_player_name,
    COUNT(1) as total_games,
    COUNT(CASE WHEN dim_did_not_play THEN 1 END) AS did_not_play_count
from fct_game_details
group by 1
ORDER BY 2 DESC;
