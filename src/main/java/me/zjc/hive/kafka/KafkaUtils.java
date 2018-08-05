package me.zjc.hive.kafka;

import kafka.common.ErrorMapping;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static kafka.api.OffsetFetchRequest.DefaultClientId;

import static me.zjc.hive.kafka.KafkaBackedTableProperties.*;

public class KafkaUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaUtils.class.getName());
    public static int CONSUMER_SO_TIMEOUT = 60000;


    public static final SimpleConsumer createSimpleConsumer(JobConf jobConf) {
        final String bootstarpServers = jobConf.get(KAFKA_BOOTSTRAP_SERVERS);
        final int fetchSize = Integer.parseInt(jobConf.get(KAFKA_FETCH_SIZE, DEFAULT_FETCH_SIZE));
        String[] arr = bootstarpServers.split(",");
        if(arr.length == 0) {
            throw new IllegalArgumentException("Kafka bootstrap servers are required");
        }
        String[] arr1 = arr[0].split(":");
        if(arr1.length == 0 || ! isNumeric(arr1[1])) {
            throw new IllegalArgumentException("Both Kafka bootstrap server host and port are required");
        }
        return new SimpleConsumer(arr1[0],
                Integer.valueOf(arr1[1]),
                CONSUMER_SO_TIMEOUT,
                fetchSize,
                DefaultClientId()
                );

    }

    public static final SimpleConsumer createSimpleConsumer(String uri, JobConf jobConf) {
        final int fetchSize = Integer.parseInt(jobConf.get(KAFKA_FETCH_SIZE, DEFAULT_FETCH_SIZE));
        final String[] arr = uri.split(":");
        return new SimpleConsumer(arr[0],
                Integer.parseInt(arr[1]),
                KafkaUtils.CONSUMER_SO_TIMEOUT,
                fetchSize,
                DefaultClientId());
    }

    /**
     * Find partition leader
     * @param replicaURI
     * @param topic
     * @param partitionId
     * @return
     */
    private static final PartitionMetadata findLeader(List<String> replicaURI, String topic, int partitionId) {
        PartitionMetadata returnMetaData = null;
        final int fetchSize = Integer.parseInt(DEFAULT_FETCH_SIZE);
        SimpleConsumer consumer = null;
        try {
            for (String server : replicaURI) {
                if (returnMetaData != null) break;
                final String[] arr = server.split(":");
                if (arr.length < 2 || !isNumeric(arr[1])) {
                    LOGGER.warn("Invalid bootstrap server " + server);
                    continue;
                }
                consumer = new SimpleConsumer(arr[0],
                        Integer.valueOf(arr[1]),
                        KafkaUtils.CONSUMER_SO_TIMEOUT,
                        fetchSize,
                        DefaultClientId());
                TopicMetadataResponse response = consumer.send(new TopicMetadataRequest(Arrays.asList(topic)));
                for (TopicMetadata topicMetadata : response.topicsMetadata()) {
                    if (returnMetaData != null) break;
                    for (PartitionMetadata part : topicMetadata.partitionsMetadata()) {
                        if (partitionId == part.partitionId() && part.errorCode() > ErrorMapping.NoError()) {
                            returnMetaData = part;
                            break;
                        }
                    }
                }
            }
        }
        catch(Exception e) {
            LOGGER.error("Error in findLeader " + e.getMessage());
        }
        finally {
            if(consumer != null) consumer.close();
            return returnMetaData;
        }
    }

    /**
     * Find new partition leader. Returns null after three attempts.
     * @param oldLeader
     * @param replicaURI
     * @param topic
     * @param partitionId
     * @return
     */
    public static PartitionMetadata findNewLeader(String oldLeader, List<String> replicaURI, String topic, int partitionId) {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader(replicaURI, topic, partitionId);
            if (metadata == null) {
                goToSleep = true;
            }
            else if (metadata.leader() == null) {
                goToSleep = true;
            }
            else if (oldLeader.equalsIgnoreCase(metadata.leader().connectionString()) && i == 0) {
                // first time through if the leader hasn't changed, give ZooKeeper a second chance to recover
                goToSleep = true;
            }
            else {
                return metadata;
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException ie) {
                    LOGGER.error("Interrupted in findNewLeader oldLeader " + oldLeader + " topic " + topic
                            + " partitionId " + partitionId);
                }
            }
        }
        LOGGER.error("Unable to find new leader");
        return null;
    }

    public static boolean isNumeric(final CharSequence cs) {
        if (cs == null || cs.length() == 0) {
            return false;
        }
        final int sz = cs.length();
        for (int i = 0; i < sz; i++) {
            if (!Character.isDigit(cs.charAt(i))) {
                return false;
            }
        }
        return true;
    }

}