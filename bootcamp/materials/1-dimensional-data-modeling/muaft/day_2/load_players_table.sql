insert into players
with years as (
    select * from GENERATE_SERIES(1980, 2022) as season
),

p as (
    select player_name, MIN(season) as first_season
    from player_seasons
    group by player_name
), 

players_and_seasons as (
    select * from p
    join years y on p.first_season <= y.season
),

windowed as (
    select 
        pas.player_name,
        pas.season,
        array_remove(
            array_agg(
                case WHEN ps.season is not null
                THEN
                    row(
                        ps.season,
                        ps.gp,
                        ps.pts,
                        ps.reb,
                        ps.ast
                    )::season_stats
                END)
                over (partition by pas.player_name order by coalesce(pas.season, ps.season)),
            NULL
        ) as seasons
    from players_and_seasons pas
    left join player_seasons ps
    on  pas.player_name = ps.player_name
    and pas.season = ps.season
    order by pas.player_name, pas.season
),

static as (
    select 
        player_name,
        max(height) as height,
        max(college) as college,
        max(country) as country,
        max(draft_year) as draft_year,
        max(draft_round) as draft_round,
        max(draft_number) as draft_number
    from player_seasons
    group by player_name
)

SELECT
    w.player_name,
    s.height,
    s.college,
    s.country,
    s.draft_year,
    s.draft_round,
    s.draft_number,
    w.seasons as season_stats,
    CASE
        WHEN w.seasons[CARDINALITY(w.seasons)].pts > 20 then 'star'
        WHEN w.seasons[CARDINALITY(w.seasons)].pts > 15 then 'good'
        WHEN w.seasons[CARDINALITY(w.seasons)].pts > 10 then 'average'
        ELSE 'bad'
    END::scoring_class as scoring_class,
    w.season - w.seasons[1].season as years_since_last_active,
    w.season as current_season,
    w.seasons[CARDINALITY(w.seasons)].season = w.season as is_active
    from windowed w
    join static s 
    on w.player_name = s.player_name
