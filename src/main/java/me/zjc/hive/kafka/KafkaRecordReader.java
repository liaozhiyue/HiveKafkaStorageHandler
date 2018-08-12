package me.zjc.hive.kafka;

import java.io.IOException;
import java.util.*;

import kafka.api.PartitionFetchInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static kafka.api.OffsetFetchRequest.DefaultClientId;

import static me.zjc.hive.kafka.KafkaBackedTableProperties.DEFAULT_FETCH_SIZE;
import static me.zjc.hive.kafka.KafkaInputFormat.CONSUMER_CORRELATION_ID;


/**
 * Refer to https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+SimpleConsumer+Example
 */
public class KafkaRecordReader implements RecordReader<NullWritable, AvroGenericRecordWritable> {
  private static Logger LOGGER = LoggerFactory.getLogger(KafkaRecordReader.class);
  private final KafkaSplit split;
  private final TopicAndPartition topicAndPartition;
  private SimpleConsumer consumer;
  private final JobConf jobConf;
  private final Reporter reporter;

  private Iterator<MessageAndOffset> buf;
  private long offset = -1;
  private long totalRecords = 1;

  /**
   * Record reader to fetch directly from Kafka
   *
   * @param split
   * @throws IOException
   * @throws InterruptedException
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public KafkaRecordReader(InputSplit split, JobConf conf, Reporter reporter) throws IOException {
    this.split = (KafkaSplit) split;
    this.jobConf = conf;
    this.reporter = reporter;
    this.consumer = KafkaUtils.createSimpleConsumer(this.split.getLeaderUri(), conf);
    this.offset = this.split.getLastReadOffset();
    this.topicAndPartition = new TopicAndPartition(this.split.getTopic(), this.split.getPartitionId());
    this.totalRecords = this.split.getLatestOffset() - this.split.getLastReadOffset();

  }

  private boolean hasNext() {
    if(offset > split.getLatestOffset()) {
      return false;
    }
    else if(buf == null || ! buf.hasNext()) {
      return fetch();
    }
    else {
      return buf.hasNext();
    }
  }

  /**
   * Fetches messages from Kafka.
   * Number of messages are specified by kafka.fetch.size.
   * @return
   */
  private boolean fetch() {
    final int fetchSize = Integer.parseInt(jobConf.get(KafkaBackedTableProperties.KAFKA_FETCH_SIZE, DEFAULT_FETCH_SIZE));
    final PartitionFetchInfo pf = new PartitionFetchInfo(offset, fetchSize);
    final String topic = split.getTopic();
    final int partitionId = split.getPartitionId();
    final LinkedHashMap<TopicAndPartition, PartitionFetchInfo> map = new LinkedHashMap<>(1);
    map.put(topicAndPartition, pf);
    FetchRequest fetchRequest = new FetchRequest(
            CONSUMER_CORRELATION_ID,
            DefaultClientId(),
            1000,
            0,
            map);

    FetchResponse response = consumer.fetch(fetchRequest);

    if (response.hasError()) {
      final short errorCode = response.errorCode(topic, partitionId);
      final Throwable t = ErrorMapping.exceptionFor(errorCode);
      LOGGER.warn("Error encountered during a fetch request from Kafka, topic " + topic
              + " partitionId " + partitionId + " offset " + offset + " group.id " + split.getGroupId()
              + " exception " + t
      );
      // Identify and recover from leader changes
      if (errorCode == ErrorMapping.LeaderNotAvailableCode()) {
        final PartitionMetadata part = KafkaUtils.findNewLeader(split.getLeaderUri(), split.getReplicaURI(), topic, partitionId);
        if (part != null ) {
          split.setLeaderUri(part.leader().connectionString());
          split.setReplicaURI(part.replicas());
          consumer.close();
          consumer = KafkaUtils.createSimpleConsumer(part.leader().connectionString(), jobConf);
          return fetch();
        }
        else {
          return false;
        }
      }
      return false;
    }
    else {
      List<MessageAndOffset> msgs = new ArrayList<>();
      Iterator<MessageAndOffset> ite = response.messageSet(topic, partitionId).iterator();
      // Skip old messages
      while(ite.hasNext()) {
        MessageAndOffset msg = ite.next();
        if(msg.offset() < offset) {
          LOGGER.debug("Message offset " + msg.offset() + " is less than topic " + topic
                  + " partitionId " + partitionId + " offset " + offset + ", skip");
        }
        else {
          msgs.add(msg);
        }
      }
      buf = msgs.iterator();
      LOGGER.info("Fetched " + msgs.size() + " messages" + " from topic " + topic + " partitionId " + partitionId
              + " group.id " + split.getGroupId());
      return buf.hasNext();
    }
  }

  /**
   * Get next message and commits offset
   * @return null if there's no more data
   */
  private MessageAndOffset getNext() {
    MessageAndOffset msg = null;
    if(buf != null && buf.hasNext()) {
      msg = buf.next();
      offset ++;
    }
    return msg;
  }


  @Override
  public void close() throws IOException {
    if (consumer != null) {
      consumer.close();
    }
  }

  @Override
  public float getProgress() throws IOException {
    if (offset >= this.split.getLatestOffset()) {
      return 1f;
    }
    return (float) ((double) getPos() / this.totalRecords);
  }


  @Override
  public long getPos() throws IOException {
      return offset - split.getLastReadOffset();
  }

  /**
   * Sets key and value. Message fetch is delegated to fetch() and getNext().
   * @param key Ignored
   * @param value where message metadata, key and value are written
   * @return
   * @throws IOException
   */
  @Override
  public boolean next(NullWritable key, AvroGenericRecordWritable value) throws IOException {
    // TODO Set readBytes and totalBytes properly.
    if(hasNext()) {
      MessageAndOffset msg = getNext();
      if(msg.message() == null) {
        return false;
      }
      KafkaAvroEncoder.encode(msg, topicAndPartition, value);
      if (value.getRecord() == null && msg.message() != null && msg.message().payload() != null) {
        throw new NullPointerException("record is missing");
      }
      return true;
    }
    else {
      return false;
    }
  }

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public AvroGenericRecordWritable createValue() {
    return new AvroGenericRecordWritable();
  }
}
