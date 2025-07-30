select 
    v.properties->>'player_name' as player_name,
    e.object_identifier,
    CAST(v.properties->>'number_of_games' AS REAL) / 
    CASE 
        WHEN CAST(v.properties->>'total_points' AS REAL) = 0 THEN 1
        ELSE CAST(v.properties->>'total_points' AS REAL)
    END,
    e.properties->>'subject_points',
    e.properties->>'num_games'
from vertices v
JOIN edges e
ON v.identifier = e.subject_identifier
AND v.type = e.subject_type
where v.type = 'player'

ORDER BY 2 DESC
