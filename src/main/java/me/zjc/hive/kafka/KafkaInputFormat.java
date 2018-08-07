package me.zjc.hive.kafka;

import java.io.IOException;
import java.util.*;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static kafka.api.OffsetFetchRequest.DefaultClientId;
import static kafka.api.OffsetRequest.CurrentVersion;
import static kafka.api.OffsetRequest.LatestTime;
import static kafka.api.OffsetRequest.EarliestTime;

import static me.zjc.hive.kafka.KafkaBackedTableProperties.*;

/**
 * Input format of Kafka Storage Handler.
 * Hive's AvroSerDe require's an AvroGenericRecordWritable.
 */
public class KafkaInputFormat implements InputFormat<NullWritable, AvroGenericRecordWritable> {
	private static Logger LOGGER = LoggerFactory.getLogger(KafkaInputFormat.class);
	public static int CONSUMER_CORRELATION_ID = 1;
	public static final String MAPRED_MAP_TASKS = "mapred.map.tasks";

	@Override
	public RecordReader getRecordReader(InputSplit split, JobConf conf, Reporter reporter)
			throws IOException {
		return new KafkaRecordReader(split, conf, reporter);
	}


	/**
	 * Returns KafkaSplits. Number of splits equals to topic partition number.
	 * @param jobConf
	 * @param numSplits
	 * @return
	 * @throws IOException
	 */
	@Override
	public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
		final String topic = jobConf.get(KAFKA_TOPIC);
		final String groupId = jobConf.get(KAFKA_GROUP_ID);
		final String offsetResetStrategy = jobConf.get(KAFKA_AUTO_OFFSET_RESET);
		final SimpleConsumer consumer = KafkaUtils.createSimpleConsumer(jobConf);
        final ArrayList<KafkaSplit> splits = new ArrayList<>();

		try {
            TopicMetadataResponse response = consumer.send(new TopicMetadataRequest(Arrays.asList(topic), CONSUMER_CORRELATION_ID));
            List<TopicMetadata> tm = response.topicsMetadata();
            for (TopicMetadata t : tm) {
                if( t.errorCode() != ErrorMapping.NoError() ) {
                    throw new IOException("Topic " + topic + " exception " + ErrorMapping.exceptionFor(t.errorCode()));
                }
                for (PartitionMetadata p : t.partitionsMetadata()) {
                    int partitionId = p.partitionId();
                    if(ErrorMapping.LeaderNotAvailableCode() == p.errorCode()) {
                        throw new IOException("Topic " + topic + " partitionId " + partitionId
                                + " error " + ErrorMapping.exceptionFor(p.errorCode())
                        );
                    }
                    // warn log for non-fatal errors
                    if (ErrorMapping.NoError() != p.errorCode()) {
                        LOGGER.warn("Receiving non-fatal error code, continuing the creation of split for topic: " + topic
                                + " partitionId " + partitionId + " exception "+ ErrorMapping.exceptionFor(p.errorCode()));
                    }

                    // Read latest offset from leader
                    final SimpleConsumer leaderConsumer = KafkaUtils.createSimpleConsumer(p.leader().connectionString(), jobConf);

                    Map<TopicAndPartition, PartitionOffsetRequestInfo> lm = new HashMap<>(1);
                    final TopicAndPartition tp = new TopicAndPartition(t.topic(), partitionId);
                    lm.put(tp, new PartitionOffsetRequestInfo(LatestTime(), 1));
                    OffsetResponse lr = leaderConsumer.getOffsetsBefore(new OffsetRequest(lm, CurrentVersion(), DefaultClientId()));
                    if( lr.hasError()) {
                        throw new IOException("Error when retrieving topic " + topic + " partitionId " + partitionId
                                + " latest offset, error code " + lr.errorCode(topic, partitionId)
                        );
                    }
                    final long latestOffset = lr.offsets(topic, partitionId)[0];

                    // Read earliest offset from leader
                    Map<TopicAndPartition, PartitionOffsetRequestInfo> em = new HashMap<>(1);
                    em.put(tp, new PartitionOffsetRequestInfo(EarliestTime(), 1) );
                    OffsetResponse er = leaderConsumer.getOffsetsBefore(new OffsetRequest(em, CurrentVersion(), DefaultClientId()));
                    if(er.hasError()) {
                        throw new IOException("Error when retrieving topic" + topic + " partitionId " + partitionId
                                + " earliest offset, error code " + lr.errorCode(topic, partitionId)
                        );
                    }
                    long earliestOffset = er.offsets(topic, partitionId)[0];

                    // Read given groupId's last offset. May not exist.
                    OffsetFetchResponse or = leaderConsumer.fetchOffsets(new OffsetFetchRequest(groupId,
                                    Arrays.asList(tp),
                                    CONSUMER_CORRELATION_ID,
                                    DefaultClientId()
                            )
                    );
                    OffsetMetadataAndError ome = or.offsets().get(tp);
                    long lastOffset = ome.offset();
                    LOGGER.debug("group.id {} last read offset {} ", groupId, lastOffset);
                    // Reset offset
                    if(lastOffset < 0 || lastOffset > latestOffset || lastOffset < earliestOffset ) {
                        LOGGER.warn("Offset out of bound topic " + topic + " partitionId " + partitionId
                                + " latestOffset " + latestOffset + " earliestOffset " + earliestOffset
                                + " group.id " + groupId + " lastOffset " + lastOffset);
                        if(offsetResetStrategy == null || offsetResetStrategy.equals(KAFKA_OFFSET_EARLIEST)) {
                            lastOffset = earliestOffset;
                        }
                        else {
                            lastOffset = latestOffset;
                        }
                    }

                    KafkaSplit split = new KafkaSplit(p.leader().connectionString(),
                            p.replicas(),
                            topic,
                            partitionId,
                            groupId,
                            earliestOffset,
                            latestOffset,
                            lastOffset,
                            new Path( jobConf.get(KafkaBackedTableProperties.TABLE_LOCATION))
                    );
                    LOGGER.info("Created KafkaSplit " + split.toString());
                    splits.add(split);
                    leaderConsumer.close();
                }
            }
        }
        catch(Exception e) {
            throw new IOException(e);
        }
        consumer.close();
        return splits.toArray(new KafkaSplit[splits.size()]);
	}

}
