
--select actorid, actor, year, count(1) from actor_films
--group by 1, 2, 3
--having count(1) > 1
--ORDER BY 3, 2,1
--
--select 
--    actor,
--    COALESCE(ARRAY_AGG(ROW(film, votes, rating, filmid, year)::film), ARRAY[]::film[]) as films,
--    COALESCE(AVG(rating), 0) as avg_rating 
--from actor_films
--where actor = 'Alain Delon'
--group by actor
--
--select * from player_seasons 
--where player_name = 'Michael Jordan'

select * from actors where quality_class = 'star' order by year DESC limit 50;

select * from actors_history_scd where current_year = 2020;