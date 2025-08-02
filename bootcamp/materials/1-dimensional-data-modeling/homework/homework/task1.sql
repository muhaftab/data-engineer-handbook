DROP TYPE film;

CREATE TYPE film AS (
    film TEXT,
    votes INT,
    rating FLOAT,
    filmid TEXT,
    year INT
);

CREATE TYPE quality_class AS ENUM (
    'star',
    'good',
    'average',
    'bad'
);

DROP TABLE actors;
CREATE TABLE IF NOT EXISTS actors (
    actor TEXT,
    films film[],
    quality_class quality_class,
    is_active BOOLEAN,
    year INT,
    PRIMARY KEY (actor, year)
);
