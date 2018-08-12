package me.zjc.hive.kafka;

import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class KafkaBackedTableProperties {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaBackedTableProperties.class);
  public static final String KAFKA_BOOTSTRAP_SERVERS = "bootstrap.servers";
  public static final String OFFSET_STR = "group.id.offset";


  public static final String KAFKA_ENABLE_AUTO_COMMIT = "enable.auto.commit";

  public static final String KAFKA_AUTO_OFFSET_RESET = "auto.offset.reset";
  public static final String KAFKA_OFFSET_EARLIEST = "earliest";
  public static final String KAFKA_OFFSET_LATEST = "latest";
  public static final String KAFKA_TOPIC = "kafka.topic";

  public static final String KAFKA_GROUP_ID = "group.id";
  public static final String KAFKA_FETCH_SIZE = "kafka.fetch.size";
  public static final String DEFAULT_FETCH_SIZE = "1048576";

  public static final String TABLE_LOCATION = "location";

  /*
   * This method initializes properties of the external table and populates
   * the jobProperties map with them so that they can be used throughout the job.
   */
  public void initialize(Properties tableProperties, Map<String,String> jobProperties,
                         TableDesc tableDesc) {

    if(! tableProperties.containsKey(KAFKA_TOPIC)) {
      throw new RuntimeException("Kafka topic is required");
    }
    if(! tableProperties.containsKey(KAFKA_BOOTSTRAP_SERVERS )) {
      throw new RuntimeException("Kafka bootstrap.servers is required");
    }
    if(! tableProperties.containsKey(KAFKA_GROUP_ID) ) {
      throw new RuntimeException("Consumer group.id is required");
    }
    if( tableProperties.containsKey(KAFKA_FETCH_SIZE) && ! KafkaUtils.isNumeric(tableProperties.getProperty(KAFKA_FETCH_SIZE))) {
      throw new RuntimeException("Fetch size must be numeric");
    }
    // Set kafka topic
    jobProperties.put(KAFKA_TOPIC, tableProperties.getProperty(KAFKA_TOPIC));

    // Set kafka bootstrap servers
    String bootstrapServers = tableProperties.getProperty(KAFKA_BOOTSTRAP_SERVERS);
    LOGGER.debug("Bootstrap.servers : " + bootstrapServers);
    jobProperties.put(KAFKA_BOOTSTRAP_SERVERS, bootstrapServers);


    // Disable automatic offset commit
    jobProperties.put(KAFKA_ENABLE_AUTO_COMMIT, "false");

    // Set auto.offset.reset, default to earliest
    jobProperties.put(KAFKA_AUTO_OFFSET_RESET,
            tableProperties.containsKey(KAFKA_AUTO_OFFSET_RESET)
            ? tableProperties.getProperty(KAFKA_AUTO_OFFSET_RESET)
            : KAFKA_OFFSET_EARLIEST);

    // Set group.id
    jobProperties.put(KAFKA_GROUP_ID, tableProperties.getProperty(KAFKA_GROUP_ID));

    // Set fetch size in each message pull request
    jobProperties.put(KAFKA_FETCH_SIZE,
            tableProperties.containsKey(KAFKA_FETCH_SIZE)
            ? tableProperties.getProperty(KAFKA_FETCH_SIZE)
            : DEFAULT_FETCH_SIZE);
    // Set table location
    jobProperties.put(TABLE_LOCATION, tableProperties.getProperty(TABLE_LOCATION));

    //Set offset
    if( tableProperties.containsKey(OFFSET_STR)) {
      jobProperties.put(OFFSET_STR, tableProperties.getProperty(OFFSET_STR));
    }

  }
}
