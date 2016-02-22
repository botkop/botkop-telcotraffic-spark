#!/usr/bin/env bash

cqlsh <<-EOF

CREATE KEYSPACE IF NOT EXISTS traffic_keyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };
USE traffic_keyspace ;

DROP TABLE IF EXISTS attach_event;
DROP TYPE IF EXISTS subscriber_type;

CREATE TYPE IF NOT EXISTS subscriber_type (
    id int,
    imsi text,
    msisdn text,
    imei text,
    last_name text,
    first_name text,
    address text,
    city text,
    zip text,
    country text
);

CREATE TABLE attach_event (
    bearer_id text,
    subscriber frozen<subscriber_type>,
    topic text,
    PRIMARY KEY (bearer_id)
);

EOF

