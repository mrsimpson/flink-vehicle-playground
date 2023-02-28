-- set up a stream which can be queried
create stream events_stream (
      -- declare the schema of the table
      	id VARCHAR,
      	eventTime BIGINT,
      	provider VARCHAR,
      	location STRUCT<
      		latitude DOUBLE,
      		longitude DOUBLE
      	>,
      	type VARCHAR,
      	newState VARCHAR
    )
WITH (
      kafka_topic='events', timestamp='eventTime',
      partitions=1,
      value_format='JSON'
    );

-- rentals
create table rentals_by_provider as
select
    provider,
    count(*) as rentals,
    windowstart as window_start,
    windowend as window_end FROM events_stream
WINDOW TUMBLING(size 5 MINUTES)
WHERE type='TRIP_START'
GROUP BY provider;

-- returns
create table returns_by_provider as
select
    provider,
    count(*) as returns,
    windowstart as window_start,
    windowend as window_end FROM events_stream
WINDOW TUMBLING(size 5 MINUTES)
WHERE type='TRIP_END'
GROUP BY provider;

-- rentals consumption
SELECT provider,
       rentals,
       TIMESTAMPTOSTRING(window_start, 'yyy-MM-dd HH:mm:ss', 'UTC') as window_start,
       TIMESTAMPTOSTRING(window_end, 'yyy-MM-dd HH:mm:ss', 'UTC') as window_end
FROM rentals_by_provider
EMIT CHANGES
LIMIT 11;