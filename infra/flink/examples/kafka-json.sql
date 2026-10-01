-- Run through SQL Gateway / SQL Client after creating both topics.
-- docker exec kafka_rlt_4_3_1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic studio_rlt_json_in --partitions 1 --replication-factor 1
-- docker exec kafka_rlt_4_3_1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic studio_rlt_json_out --partitions 1 --replication-factor 1
-- Input example: {"id":1,"payload":"hello"}
SET 'execution.target' = 'remote';
SET 'rest.address' = 'jobmanager';
SET 'rest.port' = '8081';

CREATE TABLE json_source (id INT, payload STRING) WITH (
  'connector' = 'kafka',
  'topic' = 'studio_rlt_json_in',
  'properties.bootstrap.servers' = 'kafka_rlt_4_3_1:9092',
  'properties.group.id' = 'studio_rlt_json_example',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE json_sink (id INT, payload STRING) WITH (
  'connector' = 'kafka',
  'topic' = 'studio_rlt_json_out',
  'properties.bootstrap.servers' = 'kafka_rlt_4_3_1:9092',
  'format' = 'json'
);

EXPLAIN INSERT INTO json_sink SELECT id, payload FROM json_source;
INSERT INTO json_sink SELECT id, payload FROM json_source;
