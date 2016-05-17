package com.lucidworks.hadoop.hive;

import com.lucidworks.hadoop.io.FusionOutputFormat;
import com.lucidworks.hadoop.io.LWDocumentWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class FusionHiveOutputFormat extends FusionOutputFormat
    implements HiveOutputFormat<Text, LWDocumentWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(FusionHiveOutputFormat.class);

  @Override
  public RecordWriter getHiveRecordWriter(JobConf jobConf, Path finalOutPath,
                                          Class valueClass, boolean isCompressed, Properties tableProperties,
                                          Progressable progressable) throws IOException {

    final Text text = new Text();
    final FusionRecordWriter writer = new FusionRecordWriter(jobConf, "FusionHiveWriter", progressable);
    LOG.info("Got new FusionRecordWriter for Hive");

    return new RecordWriter() {
      @Override
      public void write(Writable w) throws IOException {
        if (w instanceof LWDocumentWritable) {
          writer.write(text, (LWDocumentWritable) w);
        } else {
          throw new IOException(
              "Expected LWDocumentWritable type, but found "
                  + w.getClass().getName());
        }
      }

      @Override
      public void close(boolean abort) throws IOException {
        LOG.info("Closing FusionRecordWriter for Hive");
        writer.close(Reporter.NULL);
      }
    };
  }
}
