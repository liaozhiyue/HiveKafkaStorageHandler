package me.zjc.hive.kafka;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static me.zjc.hive.kafka.KafkaBackedTableProperties.*;


/**
 * @author Zhu Jiachuan
 */
public class KafkaRecordReaderTest {
    public static final String SCHEMA_STRING = "{"+
            "  \"type\" : \"record\",\n"+
            "  \"name\" : \"KafkaMessage\",\n"+
            "  \"namespace\" : \"me.zjc.hive.kafka.avro\",\n"+
            "  \"fields\" : [ {\n"+
            "    \"name\" : \"topic\",\n"+
            "    \"type\" : \"string\"\n"+
            "  }, {\n"+
            "    \"name\" : \"partitionId\",\n"+
            "    \"type\" : \"int\"\n"+
            "  }, {\n"+
            "    \"name\" : \"offset\",\n"+
            "    \"type\" : \"long\"\n"+
            "  }, {\n"+
            "    \"name\" : \"msgKey\",\n"+
            "    \"type\" : [ \"null\", \"string\" ],\n"+
            "    \"default\" : null\n"+
            "  }, {\n"+
            "    \"name\" : \"msgValue\",\n"+
            "    \"type\" : \"string\"\n"+
            "  } ]"+
            "}";

    @Test
    public void initAvroSerDe() throws SerDeException {
        AvroSerDe asd = new AvroSerDe();
        Properties props = new Properties();
        props.put(AvroSerdeUtils.SCHEMA_LITERAL, SCHEMA_STRING);
        props.put(serdeConstants.LIST_COLUMNS, "msgKey,msgValue");
        props.put(serdeConstants.LIST_COLUMN_TYPES, "String,String");
        props.put(AvroSerDe.TABLE_NAME, "test");
        props.put(AvroSerdeUtils.SCHEMA_NAMESPACE, "me.zjc.hive.kafka.avro");
        props.put(AvroSerdeUtils.SCHEMA_NAME, "KafkaMessage");

        SerDeUtils.initializeSerDe(asd, new Configuration(), props, null);
    }

    @Test
    public void readTest() throws IOException, SerDeException {
        AvroSerDe asd = new AvroSerDe();
        Properties props = new Properties();
        props.put(AvroSerdeUtils.SCHEMA_LITERAL, SCHEMA_STRING);
        props.put(serdeConstants.LIST_COLUMNS, "msgKey,msgValue");
        props.put(serdeConstants.LIST_COLUMN_TYPES, "String,String");
        props.put(AvroSerDe.TABLE_NAME, "test");
        props.put(AvroSerdeUtils.SCHEMA_NAMESPACE, "me.zjc.hive.kafka.avro");
        props.put(AvroSerdeUtils.SCHEMA_NAME, "KafkaMessage");

        SerDeUtils.initializeSerDe(asd, new Configuration(), props, null);

        KafkaInputFormat inputFormat = new KafkaInputFormat();
        JobConf jobConf = new JobConf();
        jobConf.set(KAFKA_BOOTSTRAP_SERVERS, "10.0.41.132:9092");
        jobConf.set(KAFKA_TOPIC, "da_nx_mon_msg");
        jobConf.set(KAFKA_GROUP_ID, "kafka-storage-handler-test-2");

        InputSplit[] splits = inputFormat.getSplits(jobConf, 1);
        assert( splits.length > 0);

        KafkaRecordReader reader = new KafkaRecordReader(splits[0], jobConf, Reporter.NULL);
        Text key = reader.createKey();
        AvroGenericRecordWritable value = reader.createValue();
        final int limitN = 2;
        int i = 0;
        while(i < limitN ) {
            reader.next(key, value);
            Object row = asd.deserialize(value);
            System.out.println(row);
            i ++;
        }
    }
}
