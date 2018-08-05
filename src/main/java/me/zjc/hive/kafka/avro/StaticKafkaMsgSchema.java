package me.zjc.hive.kafka.avro;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;


/**
 * @author Zhu Jiachuan
 */
public class StaticKafkaMsgSchema {
    private static Schema schema = null;

    static {
        schema = ReflectData.get().getSchema(KafkaMessage.class);
    }

    public static Schema schema() {
        return schema;
    }


    public static void main(String[] args) {
        System.out.println( schema.toString(true));
    }
}
