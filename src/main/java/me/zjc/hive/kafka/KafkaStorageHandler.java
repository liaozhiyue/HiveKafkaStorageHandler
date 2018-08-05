package me.zjc.hive.kafka;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;

import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class KafkaStorageHandler extends DefaultStorageHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStorageHandler.class.getName());
  private Configuration conf;

  @Override
  public void setConf(Configuration entries) {
    this.conf = entries;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return KafkaInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return KafkaOutputFormat.class;
  }

  @Override
  public Class<? extends SerDe> getSerDeClass() {
      return AvroSerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return super.getMetaHook();
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
    return super.getAuthorizationProvider();
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    Properties tableProperties = tableDesc.getProperties();
    new KafkaBackedTableProperties().initialize(tableProperties, jobProperties, tableDesc);
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    Properties tableProperties = tableDesc.getProperties();
    new KafkaBackedTableProperties().initialize(tableProperties, jobProperties, tableDesc);

  }

  /**
   * @param tableDesc
   * @param stringStringMap
   * @deprecated
   */
  @Override
  public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> stringStringMap) {
    // does nothing;
  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    // does nothing;
  }
}
