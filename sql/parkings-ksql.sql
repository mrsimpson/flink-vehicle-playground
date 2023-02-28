drop stream if exists parking_stream;
create stream parking_stream (
    f0 VARCHAR,
    f1 STRUCT<
        id VARCHAR,
        provider VARCHAR,
        location STRUCT<
              		latitude DOUBLE,
              		longitude DOUBLE
              	>,
        start BIGINT,
        "end" BIGINT,
        resolution INT,
        scheduledEnd BIGINT,
        parkingDurationMillis INT,
        availability DOUBLE
    >)
WITH (
      kafka_topic='parkings',
      partitions=1,
      value_format='JSON'
    );

-- aggregation to hour-windows
drop table if exists hourly_availability;

create table hourly_availability as
select
 f1->provider as provider,
 CAST(SUM(f1->parkingDurationMillis) AS DOUBLE)/3600000 as availability,
 windowstart as window_start,
 windowend as window_end
FROM parking_stream
WINDOW TUMBLING(size 1 HOURS)
GROUP BY
    f1->provider;

-- availability consumption
SELECT provider,
       availability,
       TIMESTAMPTOSTRING(window_start, 'yyy-MM-dd HH:mm:ss', 'UTC') as window_start,
       TIMESTAMPTOSTRING(window_end, 'yyy-MM-dd HH:mm:ss', 'UTC') as window_end
FROM hourly_availability
EMIT CHANGES;