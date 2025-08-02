--CREATE type actor_scd_type AS (
--    quality_class quality_class,
--    is_active BOOLEAN,
--    start_year INT,
--    end_year INT
--);

INSERT INTO actors_history_scd
WITH historical_scd AS (
    SELECT 
        actor,
        quality_class,
        is_active,
        start_year,
        end_year
    FROM actors_history_scd
    WHERE current_year = 2020
    AND end_year < 2020
),

previous_year_scd AS (
    SELECT *
    FROM actors_history_scd
    WHERE current_year = 2020
    AND end_year = 2020
),

this_year_data AS (
    SELECT *
    FROM actors
    WHERE year = 2021
),

unchanged_records AS (
    SELECT 
        ty.actor,
        ty.quality_class,
        ty.is_active,
        py.start_year,
        ty.year AS end_year
    FROM this_year_data ty 
    JOIN previous_year_scd py
    ON ty.actor = py.actor
    AND ty.quality_class = py.quality_class
    AND ty.is_active = py.is_active
),

changed_records_nested AS (
    SELECT 
        ty.actor,
        UNNEST(ARRAY[
            ROW(py.quality_class, py.is_active, py.start_year, ty.year)::actor_scd_type,
            ROW(ty.quality_class, ty.is_active, ty.year, ty.year)::actor_scd_type
        ]) as record
    FROM this_year_data ty 
    JOIN previous_year_scd py
    ON ty.actor = py.actor
    AND (ty.quality_class <> py.quality_class OR ty.is_active <> py.is_active)
),

changed_records AS (
    SELECT actor,
        (record::actor_scd_type).quality_class,
        (record::actor_scd_type).is_active,
        (record::actor_scd_type).start_year,
        (record::actor_scd_type).end_year
    FROM changed_records_nested
),

new_records AS (
    SELECT 
        ty.actor,
        ty.quality_class,
        ty.is_active,
        ty.year as start_year,
        ty.year AS end_year
    FROM this_year_data ty 
    LEFT JOIN previous_year_scd py
    ON ty.actor = py.actor
    WHERE py.actor IS NULL
)

SELECT *, 2021 as current_year FROM (
    SELECT * from historical_scd
    UNION ALL
    SELECT * from unchanged_records
    UNION ALL
    SELECT * from changed_records
    UNION ALL
    SELECT * from new_records
);
