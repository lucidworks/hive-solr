package com.lucidworks.hadoop.hive;

import com.lucidworks.hadoop.io.LWMapRedInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LWHiveInputFormat extends LWMapRedInputFormat {
  private static final Logger LOG = LoggerFactory.getLogger(LWHiveInputFormat.class);

  protected static class LWHiveInputSplit extends FileSplit {

    protected InputSplit inputSplit;
    protected Path path;

    public LWHiveInputSplit() {
      this(new LWInputSplit(false, false, ""), null);
    }

    public LWHiveInputSplit(InputSplit inputSplit, Path path) {
      super((Path) null, 0, 0, (String[]) null);
      this.inputSplit = inputSplit;
      this.path = path;
    }

    public InputSplit getInputSplit() {
      return inputSplit;
    }

    @Override
    public Path getPath() {
      return path;
    }

    @Override
    public long getStart() {
      return 0L;
    }

    @Override
    public long getLength() {
      try {
        return inputSplit.getLength();
      } catch (IOException ex) {
        return -1L;
      }
    }

    @Override
    public String[] getLocations() throws IOException {
      return inputSplit.getLocations();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      path = new Path(in.readUTF());
      inputSplit.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(path.toString());
      inputSplit.write(out);
    }
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    InputSplit[] realSplits = super.getSplits(job, numSplits);
    InputSplit[] dummySplits = new InputSplit[realSplits.length];

    // Try new location [hive] one first
    String path = job.get("location");
    if (path == null) {
      // Try the old one
      path = job.get("mapred.input.dir");
    }

    for (int i = 0; i < dummySplits.length; i++) {
      dummySplits[i] = new LWHiveInputSplit(realSplits[i], new Path(path));
    }

    return dummySplits;
  }

  @Override
  public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
    LWHiveInputSplit inputSplit = (LWHiveInputSplit) split;
    return super.getRecordReader(inputSplit.getInputSplit(), job, reporter);
  }
}
