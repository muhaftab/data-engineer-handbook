WITH joined AS (
    SELECT 
        gd.*,
        g.*
    FROM game_details gd
    JOIN games g ON g.game_id = gd.game_id
),

enriched AS (
    SELECT *,
    CASE 
        WHEN home_team_wins = 1 AND team_id = home_team_id THEN 1
        WHEN home_team_wins = 0 AND team_id = visitor_team_id THEN 1
        ELSE 0
    END as win
    FROM JOINED
),

grouped AS (
    SELECT 
        player_id, 
        team_id, 
        season,
        SUM(pts) AS total_points,
        SUM(CASE WHEN win = 1 THEN 1 ELSE 0 END) AS total_wins
    FROM enriched
    GROUP BY GROUPING SETS (
        (player_id, team_id),
        (player_id, season),
        (team_id)
    )
),

top_player_by_team AS (
    SELECT 
        player_id,
        team_id,
        total_points AS score,
        ROW_NUMBER() OVER(ORDER BY total_points DESC) as row_num
    FROM grouped
    WHERE total_points IS NOT NULL AND player_id IS NOT NULL AND team_id IS NOT NULL AND season IS NULL
),

top_player_by_season AS (
    SELECT 
        player_id,
        team_id,
        total_points AS score,
        ROW_NUMBER() OVER(ORDER BY total_points DESC) as row_num
    FROM grouped
    WHERE total_points IS NOT NULL AND player_id IS NOT NULL AND team_id IS NULL AND season IS NOT NULL
),

top_team_by_wins AS (
    SELECT 
        player_id,
        team_id,
        total_wins AS score,
        ROW_NUMBER() OVER(ORDER BY total_wins DESC) as row_num
    FROM grouped
    WHERE total_wins IS NOT NULL AND player_id IS NULL AND team_id IS NOT NULL AND season IS NULL
)

SELECT 'Top player by team' as metric, * FROM top_player_by_team where row_num = 1
UNION ALL
SELECT 'Top player by season' as metric, * FROM top_player_by_season where row_num = 1
UNION ALL
SELECT 'Top team by wins' as metric, * FROM top_team_by_wins where row_num = 1;

