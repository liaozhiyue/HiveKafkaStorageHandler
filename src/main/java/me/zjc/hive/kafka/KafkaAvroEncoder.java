package me.zjc.hive.kafka;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndOffset;

import me.zjc.hive.kafka.avro.KafkaMessage;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;

import java.rmi.server.UID;

import static me.zjc.hive.kafka.avro.StaticKafkaMsgSchema.schema;


/**
 * @author Zhu Jiachuan
 */
public class KafkaAvroEncoder {
    private static DatumWriter<KafkaMessage> writer = new ReflectDatumWriter<>(KafkaMessage.class);
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
        if( msg.message().key() != null) {
            record.put("msgKey", new String(msg.message().key().array()));
        }
        record.put("msgValue", msg.message().payload().array());

        avroWritable.setRecord(record);
        avroWritable.setFileSchema(schema());
        avroWritable.setRecordReaderID(RECORD_READER_ID);

    }
}
