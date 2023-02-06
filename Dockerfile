ARG FLINK_VERSION=1.16.1
FROM flink:$FLINK_VERSION

RUN mkdir /opt/flink/usrlib
ADD target/flink-vehicle-example-*.jar /opt/flink/usrlib/flink-vehicle-example.jar
