package me.zjc.hive.kafka;

import java.io.IOException;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import org.junit.Test;

import static me.zjc.hive.kafka.KafkaBackedTableProperties.*;


/**
 * @author Zhu Jiachuan
 */
public class KafkaInputFormatTest {
    @Test
    public void getInputSplitTest() throws IOException {
        KafkaInputFormat inputFormat = new KafkaInputFormat();
        JobConf jobConf = new JobConf();
        jobConf.set(KAFKA_BOOTSTRAP_SERVERS, "192.168.1.1:9092");
        jobConf.set(KAFKA_TOPIC, "a-kafka-topic");
        jobConf.set(KAFKA_GROUP_ID, "kafka-storage-handler-test-1");
        jobConf.set(KAFKA_FETCH_SIZE, "10000");


        InputSplit[] splits = inputFormat.getSplits(jobConf, 1);
        System.out.println( splits.length );
    }
}
