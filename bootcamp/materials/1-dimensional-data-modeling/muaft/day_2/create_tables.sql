drop table if exists players;


create table players (
    player_name TEXT,
    height TEXT,
    college TEXT,
    country TEXT,
    draft_year TEXT,
    draft_round TEXT,
    draft_number TEXT,
    season_stats season_stats [],
    scoring_class scoring_class,
    years_since_last_season INTEGER,
    current_season INTEGER,
    is_active BOOLEAN,
    primary key (player_name, current_season)
);

drop table if exists players_scd;
create table players_scd (
    player_name TEXT,
    scoring_class scoring_class,
    is_active BOOLEAN,
    start_season INTEGER,
    end_season INTEGER,
    current_season INTEGER,
    PRIMARY KEY (player_name, start_season)
);


create type scd_type as (
    scoring_class scoring_class,
    is_active boolean,
    start_season INTEGER,
    end_season INTEGER
); 

