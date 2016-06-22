package com.lucidworks.hadoop.hive;

import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

import static com.lucidworks.hadoop.fusion.Constants.FUSION_AUTHENABLED;
import static com.lucidworks.hadoop.fusion.Constants.FUSION_INDEX_ENDPOINT;
import static com.lucidworks.hadoop.fusion.Constants.FUSION_PASS;
import static com.lucidworks.hadoop.fusion.Constants.FUSION_QUERY_ENDPOINT;
import static com.lucidworks.hadoop.fusion.Constants.FUSION_REALM;
import static com.lucidworks.hadoop.fusion.Constants.FUSION_USER;
import static com.lucidworks.hadoop.fusion.Constants.FUSION_QUERY;
import static com.lucidworks.hadoop.fusion.Constants.FUSION_LOGIN_CONFIG;
import static com.lucidworks.hadoop.fusion.Constants.FUSION_LOGIN_APP_NAME;

public class FusionStorageHandler extends DefaultStorageHandler {

  private static final Logger LOG = LoggerFactory.getLogger(FusionStorageHandler.class);

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return FusionHiveInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return FusionHiveOutputFormat.class;
  }

  @Override
  @SuppressWarnings("deprecation")
  public Class<? extends SerDe> getSerDeClass() {
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
    String indexEndpoint = tableProps.getProperty(FUSION_INDEX_ENDPOINT);
    if (indexEndpoint != null) {
      jobProps.put(FUSION_INDEX_ENDPOINT, indexEndpoint);
    }

    String queryEndpoint = tableProps.getProperty(FUSION_QUERY_ENDPOINT);
    if (queryEndpoint != null) {
      jobProps.put(FUSION_QUERY_ENDPOINT, queryEndpoint);
    }

    String realm = tableProps.getProperty(FUSION_REALM);
    if (realm != null) {
      jobProps.put(FUSION_REALM, realm);
    }

    String pass = tableProps.getProperty(FUSION_PASS);
    if (pass != null) {
      jobProps.put(FUSION_PASS, pass);
    }

    String user = tableProps.getProperty(FUSION_USER);
    if (user != null) {
      jobProps.put(FUSION_USER, user);
    }

    String fusionQuery = tableProps.getProperty(FUSION_QUERY);
    if (fusionQuery != null) {
      jobProps.put(FUSION_QUERY, fusionQuery);
    }

    String fusionLoginConfig = tableProps.getProperty(FUSION_LOGIN_CONFIG);
    if (fusionLoginConfig != null) {
      jobProps.put(FUSION_LOGIN_CONFIG, fusionLoginConfig);
    }

    String fusionLoginAppName = tableProps.getProperty(FUSION_LOGIN_APP_NAME);
    if (fusionLoginAppName != null) {
      jobProps.put(FUSION_LOGIN_APP_NAME, fusionLoginAppName);
    }

    String auth = tableProps.getProperty(FUSION_AUTHENABLED, "true");
    jobProps.put(FUSION_AUTHENABLED, auth);
  }
}
