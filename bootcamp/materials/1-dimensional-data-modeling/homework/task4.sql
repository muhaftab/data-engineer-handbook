INSERT INTO actors_history_scd
WITH previous AS (
    SELECT actor,
        year,
        quality_class,
        is_active,
        LAG(quality_class, 1) OVER (PARTITION BY actor ORDER BY year) as previous_quality_class,
        LAG(is_active, 1) OVER (PARTITION BY actor ORDER BY year) as previous_is_active
    FROM actors
),

indicators AS (
    SELECT *, 
        CASE 
            WHEN quality_class <> previous_quality_class THEN 1
            WHEN is_active <> previous_is_active THEN 1
            ELSE 0
        END as change_indicator 
    FROM previous
),

streaks AS (
    SELECT *,
        SUM(change_indicator) OVER (PARTITION BY actor ORDER BY year) AS streak_identifier
    FROM indicators
)

SELECT 
    actor,
    quality_class,
    is_active,
    MIN(year) AS start_year,
    MAX(year) AS end_year,
    2020 as current_year
FROM streaks
GROUP BY actor, streak_identifier, quality_class, is_active
ORDER BY actor, streak_identifier;



