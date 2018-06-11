package com.lucidworks.hadoop.hive;

import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;

import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

import static com.lucidworks.hadoop.hive.HiveSolrConstants.COMMIT_ON_CLOSE;
import static com.lucidworks.hadoop.hive.HiveSolrConstants.ENABLE_FIELD_MAPPING;
import static com.lucidworks.hadoop.hive.HiveSolrConstants.SOLR_COLLECTION;
import static com.lucidworks.hadoop.hive.HiveSolrConstants.SOLR_QUERY;
import static com.lucidworks.hadoop.hive.HiveSolrConstants.SOLR_SERVER_URL;
import static com.lucidworks.hadoop.hive.HiveSolrConstants.SOLR_ZKHOST;

public class LWStorageHandler extends DefaultStorageHandler {

  private static final Logger LOG = LoggerFactory.getLogger(LWStorageHandler.class);

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return LWHiveInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return LWHiveOutputFormat.class;
  }

  @Override
  @SuppressWarnings("deprecation")
  public Class<? extends AbstractSerDe> getSerDeClass() {
    return LWSerDe.class;
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc,
          Map<String, String> jobProperties) {

    setProperties(tableDesc.getProperties(), jobProperties);
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc,
          Map<String, String> jobProperties) {

    setProperties(tableDesc.getProperties(), jobProperties);
  }

  private void setProperties(Properties tableProps, Map<String, String> jobProps) {
    // Connection string(s)
    String zkHost = tableProps.getProperty(SOLR_ZKHOST);
    if (zkHost != null) {
      jobProps.put(SOLR_ZKHOST, zkHost);
    } else {
      String solrServerUrl = tableProps.getProperty(SOLR_SERVER_URL);
      if (solrServerUrl != null) {
        jobProps.put(SOLR_SERVER_URL, solrServerUrl);
      }
    }

    // Collection
    String collection = tableProps.getProperty(SOLR_COLLECTION);
    if (collection != null) {
      jobProps.put(SOLR_COLLECTION, collection);
    }

    // Query
    String query = tableProps.getProperty(SOLR_QUERY);
    if (query != null) {
      jobProps.put(SOLR_QUERY, query);
    }

    // Commit on close [yes]
    String commit = tableProps.getProperty(COMMIT_ON_CLOSE, "true");
    jobProps.put(COMMIT_ON_CLOSE, commit);

    String enableFieldMapping = tableProps.getProperty(ENABLE_FIELD_MAPPING, "false");
    jobProps.put(ENABLE_FIELD_MAPPING, enableFieldMapping);
  }
}
