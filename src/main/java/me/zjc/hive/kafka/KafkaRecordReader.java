package me.zjc.hive.kafka;

import java.io.IOException;
import java.util.*;

import kafka.api.PartitionFetchInfo;
import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetCommitRequest;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static kafka.api.OffsetFetchRequest.DefaultClientId;
import static kafka.api.OffsetRequest.CurrentVersion;

import static me.zjc.hive.kafka.KafkaBackedTableProperties.DEFAULT_FETCH_SIZE;
import static me.zjc.hive.kafka.KafkaInputFormat.CONSUMER_CORRELATION_ID;


/**
 * Refer to https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+SimpleConsumer+Example
 */
public class KafkaRecordReader implements RecordReader<Text, AvroGenericRecordWritable> {
  private static Logger LOGGER = LoggerFactory.getLogger(KafkaRecordReader.class);
  private final KafkaSplit split;
  private final TopicAndPartition topicAndPartition;
  private SimpleConsumer consumer;
  private final JobConf jobConf;
  private final Reporter reporter;

  private Iterator<MessageAndOffset> buf;
  private long offset = -1;

  private long totalBytes = 1;
  private long readBytes = 0;

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
  }

  private boolean hasNext() {
    if(buf == null || ! buf.hasNext()) {
      return fetch();
    }
    else {
      return buf.hasNext();
    }
  }

  /**
   * Fetches messages from Kafka.
   * Number of messages are specified by kafka.fetch.size, defaults to 1024 * 1024.
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
              + " group.id" + split.getGroupId());
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
      OffsetAndMetadata om = OffsetAndMetadata.apply(offset);
      Map<TopicAndPartition, OffsetAndMetadata> commitMap = new LinkedHashMap<>(1);
      commitMap.put(topicAndPartition, om);
      consumer.commitOffsets(new OffsetCommitRequest(split.getGroupId(),
              commitMap,
              CONSUMER_CORRELATION_ID,
              DefaultClientId(),
              CurrentVersion())
      );
    }
    return msg;
  }


  @Override
  public synchronized void close() throws IOException {
    if (consumer != null) {
      consumer.close();
    }
  }

  @Override
  public float getProgress() throws IOException {
    if (getPos() == 0) {
      return 0f;
    }

    if (getPos() >= totalBytes) {
      return 1f;
    }
    return (float) ((double) getPos() / totalBytes);
  }


  @Override
  public long getPos() throws IOException {
      return readBytes;
  }

  /**
   * Sets key and value. Message fetch and offset commitment are delegated to fetch() and getNext().
   * @param key Ignored
   * @param value where message metadata, key and value are written
   * @return
   * @throws IOException
   */
  @Override
  public boolean next(Text key, AvroGenericRecordWritable value) throws IOException {
    // TODO Set readBytes and totalBytes properly.
    if(hasNext()) {
      MessageAndOffset msg = getNext();
      if(msg.message() == null) {
        return false;
      }

      KafkaAvroEncoder.encode(msg, topicAndPartition, value);
      return true;
    }
    else {
      return false;
    }
  }

  @Override
  public Text createKey() {
    return new Text();
  }

  @Override
  public AvroGenericRecordWritable createValue() {
    return new AvroGenericRecordWritable();
  }
}
