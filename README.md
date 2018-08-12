# HiveKafkaStorageHandler

## Dependencies
- Kafka 0.10.x
- Java 1.7

## Example
1. Put following jars in HIVE_AUX_JARS_PATH
- kafka_2.11-0.10.1.1.jar
- scala-library-2.11.8.jar
- kafka-clients-0.10.1.1.jar

2. Run hive CLI and ADD jar /path/to/HiveKafkaStorageHandler.jar
3. Create Hive external table
````
CREATE EXTERNAL TABLE table_1 (
  msgKey STRING,
  msgValue STRING
)
STORED BY 'me.zjc.hive.kafka.KafkaStorageHandler'
WITH SERDEPROPERTIES (
  "bootstrap.servers" = "192.168.1.1:9092",
  "kafka.topic" = "topic_name",
  "group.id" = "test_group_id",
  "auto.offset.reset"="earliest",
  'avro.schema.literal' = '{
    "type" : "record",
    "name" : "KafkaMessage",
    "namespace" : "me.zjc.hive.kafka.avro",
    "fields" : [ {
    "name" : "topic",
    "type" : "string"
  }, {
    "name" : "partitionId",
    "type" : "int"
  }, {
    "name" : "offset",
    "type" : "long"
  }, {
    "name" : "msgKey",
    "type" : [ "null", "string" ],
    "default" : null
  }, {
    "name" : "msgValue",
    "type" : "string"
  } ]
}'
);
````
4. Run query:
````
SELECT * FROM table_1 LIMIT 1;
````


