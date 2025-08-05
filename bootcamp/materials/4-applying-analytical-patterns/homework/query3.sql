/*
- A query that uses window functions on `game_details` to find out the following things:
  - What is the most games a team has won in a 90 game stretch? 
  - How many games in a row did LeBron James score over 10 points a game?
*/

WITH joined AS (
    SELECT 
        gd.team_id,
        gd.game_id,
        gd.player_name,
        gd.pts,
        g.home_team_id,
        g.visitor_team_id,
        g.home_team_wins,
        g.season,
        g.game_date_est
    FROM game_details gd
    JOIN games g ON g.game_id = gd.game_id
),

enriched AS (
    SELECT *,
        CASE 
            WHEN home_team_wins = 1 AND team_id = home_team_id THEN 1
            WHEN home_team_wins = 0 AND team_id = visitor_team_id THEN 1
            ELSE 0
        END as team_won
    FROM JOINED
),

-- For Question 1: Most team wins in a 90-game stretch

team_games AS (
    SELECT 
        team_id,
        game_Id,
        MAX(team_won) as team_won,
        MAX(game_date_est) as game_date_est
    FROM enriched
    GROUP BY team_id, game_id
),

team_wins_90_games AS (
    SELECT 
        team_id,
        SUM(team_won) OVER(PARTITION BY team_id ORDER BY game_date_est ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) as wins_in_90_games,
        COUNT(1) OVER(PARTITION BY team_id ORDER BY game_date_est ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) as games_in_window
    FROM team_games
),

-- For Question 2: LeBron consecutive games with 10+ points
lebron_games AS (
    SELECT 
        player_name,
        game_id,
        game_date_est,
        pts as player_points
    FROM enriched
    WHERE player_name = 'LeBron James'
),

lebron_streaks AS (
    SELECT 
        player_name,
        game_date_est,
        player_points,
        CASE WHEN player_points >= 10 THEN 1 ELSE 0 END as scored_10_plus,
        ROW_NUMBER() OVER (ORDER BY game_date_est) - 
        ROW_NUMBER() OVER (PARTITION BY CASE WHEN player_points >= 10 THEN 1 ELSE 0 END ORDER BY game_date_est) as streak_group
    FROM lebron_games
),

lebron_consecutive AS (
    SELECT 
        streak_group,
        scored_10_plus,
        COUNT(*) as consecutive_games
    FROM lebron_streaks
    WHERE scored_10_plus = 1
    GROUP BY streak_group, scored_10_plus
),

-- Answer Question 1
question1 AS (
    SELECT 
        'Question 1: Most wins in 90-game stretch' as question,
        team_id,
        MAX(wins_in_90_games) as max_wins_in_90_games
    FROM team_wins_90_games
    WHERE games_in_window = 90
    GROUP BY team_id
    ORDER BY max_wins_in_90_games DESC
    LIMIT 1
),

-- Answer Question 2
question2 AS (
    SELECT 
        'Question 2: LeBron consecutive 10+ point games' as question,
        CAST(NULL as INT) as team_id,
        MAX(consecutive_games) as max_consecutive_games
    FROM lebron_consecutive
)

-- Final results
SELECT question, team_id, max_wins_in_90_games as result FROM question1
UNION ALL
SELECT question, team_id, max_consecutive_games as result FROM question2;