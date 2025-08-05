DO $$
DECLARE
    start_season INT := 1996;
    end_season INT := 2021;
    curr_season INT := start_season;
BEGIN
    WHILE curr_season < end_season LOOP
        insert into players
        with yesterday as (
            select *
            from players
            where current_season = curr_season
        ),
        today as (
            select *
            from player_seasons
            where season = curr_season + 1
        )
        select COALESCE(t.player_name, y.player_name) as player_name,
            COALESCE(t.height, y.height) as height,
            COALESCE(t.college, y.college) as college,
            COALESCE(t.country, y.country) as country,
            COALESCE(t.draft_year, y.draft_year) as draft_year,
            COALESCE(t.draft_round, y.draft_round) as draft_round,
            COALESCE(t.draft_number, y.draft_number) as draft_number,
            CASE 
                WHEN y.season_stats is NULL
                THEN ARRAY[ROW(
                    t.season,
                    t.gp,
                    t.pts,
                    t.reb,
                    t.ast
                )::season_stats]
                WHEN t.season is not NULL
                then  y.season_stats || ARRAY[ROW(
                    t.season,
                    t.gp,
                    t.pts,  
                    t.reb,
                    t.ast
                )::season_stats]
                ELSE y.season_stats
            END as season_stats,
            CASE
                WHEN t.season is not NULL
                    THEN CASE
                        WHEN t.pts >= 20 THEN 'star'
                        WHEN t.pts >= 15 THEN 'good'
                        WHEN t.pts >= 10 THEN 'average'
                        ELSE 'bad'
                    END::scoring_class
                ELSE y.scoring_class
            END as scoring_class,
            CASE
                WHEN t.season is not NULL
                    THEN 0
                ELSE COALESCE(y.years_since_last_season, 0) + 1
            END as years_since_last_season,
            COALESCE(t.season, y.current_season + 1) as current_season
        from today t
            full outer join yesterday y on t.player_name = y.player_name;
        curr_season := curr_season + 1;
    END LOOP;
END $$;

--select * from players where current_season = 2001 and player_name = 'Michael Jordan';

--with unnested as (
--select player_name, unnest(season_stats) as season_stats from players
--where current_season = 2001 --and player_name = 'Michael Jordan'
--)
--select player_name,
--(season_stats).*
-- from unnestedz/.=-

select player_name, season_stats[cardinality(season_stats)].pts / 
case when season_stats[1].pts = 0 then 1 else season_stats[1].pts end
from players 
where current_season = 2001
and scoring_class = 'star'
order by 2 desc;