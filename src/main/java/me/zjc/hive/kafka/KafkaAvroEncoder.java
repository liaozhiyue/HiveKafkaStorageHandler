package me.zjc.hive.kafka;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndOffset;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.rmi.server.UID;

import static me.zjc.hive.kafka.avro.StaticKafkaMsgSchema.schema;


/**
 * @author Zhu Jiachuan
 */
public class KafkaAvroEncoder {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAvroEncoder.class);

    // Hive's AvroSerDe caches each Avro Schema Encoder and all kafka messages share one schema.
    // It's safe to use one recordReaderId
    private static final UID RECORD_READER_ID = new UID((short)16);
    /**
     * Use an avro schema to encode kafka message
     * @param msg
     * @return
     */
    public static void encode(MessageAndOffset msg,
                              TopicAndPartition tp,
                              AvroGenericRecordWritable avroWritable) {
        GenericRecord record = new GenericData.Record(schema());
        record.put("topic", tp.topic());
        record.put("partitionId", tp.partition());
        record.put("offset", msg.offset());

        ByteBuffer key = msg.message().key();
        byte[] msgKey = new byte[key.limit()];
        key.get(msgKey);

        ByteBuffer payload = msg.message().payload();
        final byte[] msgValue = new byte[payload.limit()];
        payload.get(msgValue);

        try {
            if( msg.message().key() != null) {
                record.put("msgKey", new String(msgKey, "UTF-8"));
            }
            record.put("msgValue", new String(msgValue, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        LOGGER.debug("Generated GenericRecord from topic {} partitionId {} offset {}",
                tp.topic(),
                tp.partition(),
                msg.offset()
        );
        avroWritable.setRecord(record);
        avroWritable.setFileSchema(schema());
        avroWritable.setRecordReaderID(RECORD_READER_ID);
    }
}
