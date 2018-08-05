package me.zjc.hive.kafka.avro;

import org.apache.avro.reflect.Nullable;

/**
 * @author Zhu Jiachuan
 */
public class KafkaMessage {
    private String topic;
    private Integer partitionId;
    private Long offset;
    @Nullable
    private String msgKey;
    private String msgValue;

    public KafkaMessage() {
    }

    public KafkaMessage(String topic, Integer partitionId, Long offset, String msgKey, String msgValue) {
        this.topic = topic;
        this.partitionId = partitionId;
        this.offset = offset;
        this.msgKey = msgKey;
        this.msgValue = msgValue;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(Integer partitionId) {
        this.partitionId = partitionId;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public String getMsgKey() {
        return msgKey;
    }

    public void setMsgKey(String msgKey) {
        this.msgKey = msgKey;
    }

    public String getMsgValue() {
        return msgValue;
    }

    public void setMsgValue(String msgValue) {
        this.msgValue = msgValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof KafkaMessage)) return false;

        KafkaMessage that = (KafkaMessage) o;

        if (topic != null ? !topic.equals(that.topic) : that.topic != null) return false;
        if (partitionId != null ? !partitionId.equals(that.partitionId) : that.partitionId != null) return false;
        if (offset != null ? !offset.equals(that.offset) : that.offset != null) return false;
        if (msgKey != null ? !msgKey.equals(that.msgKey) : that.msgKey != null) return false;
        return msgValue != null ? msgValue.equals(that.msgValue) : that.msgValue == null;
    }

    @Override
    public int hashCode() {
        int result = topic != null ? topic.hashCode() : 0;
        result = 31 * result + (partitionId != null ? partitionId.hashCode() : 0);
        result = 31 * result + (offset != null ? offset.hashCode() : 0);
        result = 31 * result + (msgKey != null ? msgKey.hashCode() : 0);
        result = 31 * result + (msgValue != null ? msgValue.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "KafkaMessage{" +
                "topic='" + topic + '\'' +
                ", partitionId=" + partitionId +
                ", offset=" + offset +
                ", msgKey='" + msgKey + '\'' +
                ", msgValue='" + msgValue + '\'' +
                '}';
    }
}
