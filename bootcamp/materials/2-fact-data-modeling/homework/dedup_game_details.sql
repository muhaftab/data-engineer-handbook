WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id ORDER BY game_id) as row_num
    FROM game_details
)
SELECT * FROM deduped
WHERE row_num = 1;

