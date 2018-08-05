package me.zjc.hive.kafka;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class KafkaOutputFormat implements OutputFormat{
  @Override
  public RecordWriter getRecordWriter(FileSystem fileSystem, JobConf jobConf, String s,
                                      org.apache.hadoop.util.Progressable progressable) throws
      IOException {
    //TODO Finish this
    return null;
  }

  @Override
  public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {

  }
}