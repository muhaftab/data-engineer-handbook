CREATE TABLE IF NOT EXISTS actors_history_scd (
    actor TEXT,
    quality_class quality_class,
    is_active BOOLEAN,
    start_year INT,
    end_year INT,
    current_year INT,
    PRIMARY KEY(actor, start_year, current_year)
);