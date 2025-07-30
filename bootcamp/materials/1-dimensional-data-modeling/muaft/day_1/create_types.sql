create type season_stats as (
    season INTEGER,
    gp INTEGER,
    pts REAL,
    reb REAL,
    ast REAL
);

create TYPE scoring_class as ENUM (
    'star',
    'good',
    'average',
    'bad'
);
