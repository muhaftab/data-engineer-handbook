create type vertex_type as ENUM (
    'player',
    'team',
    'game'
);

create table vertices (
    identifier TEXT,
    type vertex_type,
    properties JSON,
    primary key (identifier, type)
);

create type edge_type as ENUM (
    'plays_against',
    'shares_team',
    'plays_in',
    'plays_on'
);

create table edges (
    subject_identifier TEXT,
    subject_type vertex_type,
    object_identifier TEXT,
    object_type vertex_type,
    edge_type edge_type,
    properties JSON,
    primary key (subject_identifier, subject_type, object_identifier, object_type, edge_type)
);

