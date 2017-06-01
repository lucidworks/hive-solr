package com.lucidworks.hadoop.hive;

import org.apache.hadoop.hive.serde2.SerDeException;

import java.util.Date;

import static com.lucidworks.hadoop.hive.HiveSolrConstants.SOLR_SUFFIX_BOOLEAN;
import static com.lucidworks.hadoop.hive.HiveSolrConstants.SOLR_SUFFIX_DATE;
import static com.lucidworks.hadoop.hive.HiveSolrConstants.SOLR_SUFFIX_DOUBLE;
import static com.lucidworks.hadoop.hive.HiveSolrConstants.SOLR_SUFFIX_FLOAT;
import static com.lucidworks.hadoop.hive.HiveSolrConstants.SOLR_SUFFIX_INTEGER;
import static com.lucidworks.hadoop.hive.HiveSolrConstants.SOLR_SUFFIX_LONG;
import static com.lucidworks.hadoop.hive.HiveSolrConstants.SOLR_SUFFIX_MULTI_BOOLEAN;
import static com.lucidworks.hadoop.hive.HiveSolrConstants.SOLR_SUFFIX_MULTI_DATE;
import static com.lucidworks.hadoop.hive.HiveSolrConstants.SOLR_SUFFIX_MULTI_DOUBLE;
import static com.lucidworks.hadoop.hive.HiveSolrConstants.SOLR_SUFFIX_MULTI_FLOAT;
import static com.lucidworks.hadoop.hive.HiveSolrConstants.SOLR_SUFFIX_MULTI_INTEGER;
import static com.lucidworks.hadoop.hive.HiveSolrConstants.SOLR_SUFFIX_MULTI_LONG;
import static com.lucidworks.hadoop.hive.HiveSolrConstants.SOLR_SUFFIX_MULTI_STRING;
import static com.lucidworks.hadoop.hive.HiveSolrConstants.SOLR_SUFFIX_STRING;

public class FieldMappingHelper {
  public static String fieldMapping(String fieldName, Object item) throws SerDeException {
    String suffix;

    if (item instanceof String) {
      suffix = SOLR_SUFFIX_STRING;
    } else if (item instanceof Boolean) {
      suffix = SOLR_SUFFIX_BOOLEAN;
    } else if (item instanceof Double) {
      suffix = SOLR_SUFFIX_DOUBLE;
    } else if (item instanceof Integer) {
      suffix = SOLR_SUFFIX_INTEGER;
    } else if (item instanceof Long) {
      suffix = SOLR_SUFFIX_LONG;
    } else if (item instanceof Float) {
      suffix = SOLR_SUFFIX_FLOAT;
    } else if (item instanceof Date) {
      suffix = SOLR_SUFFIX_DATE;
    } else {
      throw new SerDeException("Invalid data type");
    }

    return fieldName + suffix;
  }

  public static String multiFieldMapping(String fieldName, Object item) throws SerDeException {
    String suffix;

    if (item instanceof String) {
      suffix = SOLR_SUFFIX_MULTI_STRING;
    } else if (item instanceof Boolean) {
      suffix = SOLR_SUFFIX_MULTI_BOOLEAN;
    } else if (item instanceof Double) {
      suffix = SOLR_SUFFIX_MULTI_DOUBLE;
    } else if (item instanceof Integer) {
      suffix = SOLR_SUFFIX_MULTI_INTEGER;
    } else if (item instanceof Long) {
      suffix = SOLR_SUFFIX_MULTI_LONG;
    } else if (item instanceof Float) {
      suffix = SOLR_SUFFIX_MULTI_FLOAT;
    } else if (item instanceof Date) {
      suffix = SOLR_SUFFIX_MULTI_DATE;
    } else {
      throw new SerDeException("Invalid data type");
    }

    return fieldName + suffix;
  }
}
