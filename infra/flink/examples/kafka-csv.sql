-- Create studio_rlt_csv_in / studio_rlt_csv_out topics before execution.
-- Input example: 1,hello
SET 'execution.target' = 'remote';
SET 'rest.address' = 'jobmanager';
SET 'rest.port' = '8081';

CREATE TABLE csv_source (id INT, payload STRING) WITH (
  'connector' = 'kafka',
  'topic' = 'studio_rlt_csv_in',
  'properties.bootstrap.servers' = 'kafka_rlt_4_3_1:9092',
  'properties.group.id' = 'studio_rlt_csv_example',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'csv'
);

CREATE TABLE csv_sink (id INT, payload STRING) WITH (
  'connector' = 'kafka',
  'topic' = 'studio_rlt_csv_out',
  'properties.bootstrap.servers' = 'kafka_rlt_4_3_1:9092',
  'format' = 'csv'
);

EXPLAIN INSERT INTO csv_sink SELECT id, payload FROM csv_source;
INSERT INTO csv_sink SELECT id, payload FROM csv_source;
