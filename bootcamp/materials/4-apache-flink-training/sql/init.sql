-- Create processed_events table
CREATE TABLE IF NOT EXISTS processed_events(
    ip VARCHAR,
    event_timestamp TIMESTAMP(3),
    referrer VARCHAR,
    host VARCHAR,
    url VARCHAR,
    geodata VARCHAR
);

CREATE TABLE IF NOT EXISTS processed_events_aggregated (
    event_hour TIMESTAMP(3),
    host VARCHAR,
    num_hits BIGINT
);

CREATE TABLE IF NOT EXISTS processed_events_aggregated_referred (
    event_hour TIMESTAMP(3),
    host VARCHAR,
    referrer VARCHAR,
    num_hits BIGINT
);

INSERT INTO processed_events_aggregated VALUES (CURRENT_TIMESTAMP, 'testhost', 1)

drop table processed_events_test;
CREATE TABLE IF NOT EXISTS processed_events_test(
    ip VARCHAR,
    event_timestamp TIMESTAMP(3),
    referrer VARCHAR,
    host VARCHAR,
    url VARCHAR,
    geodata VARCHAR
);


SELECT * from processed_events_aggregated;
SELECT * from processed_events_aggregated_referred;
