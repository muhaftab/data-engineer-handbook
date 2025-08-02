
INSERT INTO actors
with last_year AS (
    SELECT * FROM actors
    WHERE year = 1969
),

this_year AS (
    select 
        actor,
        COUNT(1) as num_films,
        ARRAY_AGG(ROW(film, votes, rating, filmid, year)::film) as films,
        AVG(rating) as avg_rating,
        year
    from actor_films
    where year = 1970
    GROUP BY actor, year
) 

select 
    COALESCE(ty.actor, ly.actor) as actor,
    CASE 
        WHEN ty.actor IS NOT NULL AND ly.actor IS NULL THEN ty.films
        WHEN ty.actor IS NULL AND ly.actor IS NOT NULL THEN ly.films
        ELSE ly.films || ty.films
    END as films,
    CASE 
        WHEN ty.actor IS NOT NULL THEN
            CASE 
                WHEN ty.avg_rating > 8 THEN 'star'
                WHEN ty.avg_rating > 7 AND ty.avg_rating <= 8 THEN 'good'
                WHEN ty.avg_rating > 6 AND ty.avg_rating <= 7 THEN 'average'
                ELSE 'bad'
            END::quality_class 
        ELSE ly.quality_class
    END AS quality_class,
    CASE 
        WHEN ty.actor IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_active,
    COALESCE(ty.year, ly.year+1) as year
from this_year ty
FULL OUTER JOIN last_year ly
ON ty.actor = ly.actor;