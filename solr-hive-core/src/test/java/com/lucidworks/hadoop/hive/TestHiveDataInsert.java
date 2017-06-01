package com.lucidworks.hadoop.hive;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.lucidworks.hadoop.utils.SolrCloudClusterSupport;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(StandaloneHiveRunner.class)
public class TestHiveDataInsert extends SolrCloudClusterSupport {

  private static final Logger LOG = Logger.getLogger(TestHiveDataInsert.class);

  @HiveSQL(files = {})
  private HiveShell shell;

  private SolrClient solrClient;

  @Before
  public void before() {
    shell.execute("create table temporary_table(a string);");
    shell.execute("insert into table temporary_table values ('a');");
    solrClient = new CloudSolrClient.Builder().withZkHost(getZkHost()).withZkChroot("/solr").build();
  }

  @After
  public void after() throws IOException, SolrServerException {
    removeAllDocs();
  }

  private static String createTableCommand(String tableName, String columns, boolean enableFieldMapping) {
    //@formatter:off
    StringBuilder command = new StringBuilder()
      .append("CREATE EXTERNAL TABLE ").append(tableName)
      .append(" (").append(columns).append(") ")
      .append("STORED BY 'com.lucidworks.hadoop.hive.LWStorageHandler' ")
      .append("LOCATION '/tmp/solr' ")
      .append("TBLPROPERTIES(")
      .append("'solr.zkhost' = '").append(getZkHost()).append("/solr', ")
      .append("'solr.collection' = '").append(DEFAULT_COLLECTION).append("', ")
      .append("'solr.query' = '*:*', ")
      .append("'enable.field.mapping' = '").append(String.valueOf(enableFieldMapping))
      .append("');");
    //@formatter:on

    return command.toString();
  }

  @Test
  public void testDataInsertWithoutFieldMapping() throws Exception {
    String tableName = "solr0";

    shell.execute(createTableCommand(tableName, "id string, col1 string, col2 boolean, col3 int", false));
    shell.execute("insert into " + tableName + " values ('id0', 'abc', true, 12), ('id1', 'def', false, 66);");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");

    try {
      QueryResponse queryResponse = solrClient.query(DEFAULT_COLLECTION, params);
      SolrDocumentList solrDocumentList = queryResponse.getResults();
      assertEquals(2, solrDocumentList.size());

      params = new ModifiableSolrParams();
      params.add("q", "id:\"id0\"");
      queryResponse = solrClient.query(DEFAULT_COLLECTION, params);
      solrDocumentList = queryResponse.getResults();
      assertEquals(1, solrDocumentList.size());
      SolrDocument solrDocument = solrDocumentList.get(0);

      assertTrue(solrDocument.containsKey("id"));
      assertTrue(solrDocument.containsKey("col1"));
      assertTrue(solrDocument.containsKey("col2"));
      assertTrue(solrDocument.containsKey("col3"));

      assertEquals("id0", solrDocument.get("id"));
      assertEquals(Arrays.asList("abc"), solrDocument.get("col1"));
      assertEquals(Arrays.asList(true), solrDocument.get("col2"));
      assertEquals(Arrays.asList(Long.valueOf(12)), solrDocument.get("col3"));
    } catch (SolrServerException e) {
      throw e;
    } catch (IOException e) {
      throw e;
    }
  }

  @Test
  public void testDataInsertWithFieldMapping() throws Exception {
    String tableName = "solr1";

    shell.execute(createTableCommand(tableName, "id string, col1 string, col2 boolean, col3 int", true));
    shell.execute("insert into " + tableName + " values ('id0', 'abc', true, 12), ('id1', 'def', false, 66);");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");

    try {
      QueryResponse queryResponse = solrClient.query(DEFAULT_COLLECTION, params);
      SolrDocumentList solrDocumentList = queryResponse.getResults();
      assertEquals(2, solrDocumentList.size());

      params = new ModifiableSolrParams();
      params.add("q", "id:\"id0\"");
      queryResponse = solrClient.query(DEFAULT_COLLECTION, params);
      solrDocumentList = queryResponse.getResults();
      assertEquals(1, solrDocumentList.size());
      SolrDocument solrDocument = solrDocumentList.get(0);

      assertTrue(solrDocument.containsKey("id"));
      assertTrue(solrDocument.containsKey("col1_s"));
      assertTrue(solrDocument.containsKey("col2_b"));
      assertTrue(solrDocument.containsKey("col3_i"));

      assertEquals("id0", solrDocument.get("id"));
      assertEquals("abc", solrDocument.get("col1_s"));
      assertEquals(true, solrDocument.get("col2_b"));
      assertEquals(12, solrDocument.get("col3_i"));
    } catch (SolrServerException e) {
      throw e;
    } catch (IOException e) {
      throw e;
    }
  }

  @Test
  public void testAutoGeneratedIdFieldByUUID() throws Exception {
    String tableName = "solr2";

    shell.execute(createTableCommand(tableName, "col1 string, col2 boolean, col3 int", true));
    shell.execute(
      "insert into " + tableName + " values ('bvbvb', true, 88), ('vvbvv', true, 78), ('tytyty', false, " + "56);");

    ModifiableSolrParams qparams = new ModifiableSolrParams();
    qparams.add("q", "*:*");

    try {
      QueryResponse queryResponse = solrClient.query(DEFAULT_COLLECTION, qparams);
      SolrDocumentList solrDocumentList = queryResponse.getResults();
      assertEquals(3, solrDocumentList.size());

      for (SolrDocument solrDocument : solrDocumentList) {
        assertTrue(solrDocument.containsKey("id"));
      }
    } catch (SolrServerException e) {
      throw e;
    } catch (IOException e) {
      throw e;
    }
  }

  @Test
  public void testDataInsertArrayWithPrimitiveFields() throws Exception {
    String tableName = "solr3";

    shell.execute(createTableCommand(tableName, "id string, col1 array<string>", true));
    shell.execute("insert into " + tableName + " select 'id0', array('abc', 'def', 'ghi') from temporary_table;");
    shell.execute("insert into " + tableName + " select 'id1', array('123', '456', '789') from temporary_table;");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");

    try {
      QueryResponse queryResponse = solrClient.query(DEFAULT_COLLECTION, params);
      SolrDocumentList solrDocumentList = queryResponse.getResults();
      assertEquals(2, solrDocumentList.size());

      params = new ModifiableSolrParams();
      params.add("q", "id:\"id1\"");
      queryResponse = solrClient.query(DEFAULT_COLLECTION, params);
      solrDocumentList = queryResponse.getResults();
      assertEquals(1, solrDocumentList.size());
      SolrDocument solrDocument = solrDocumentList.get(0);

      assertEquals(Arrays.asList("123", "456", "789"), solrDocument.get("col1_ss"));
    } catch (SolrServerException e) {
      throw e;
    } catch (IOException e) {
      throw e;
    }
  }

  @Test
  public void testDataInsertArrayWithNestedArrayField() throws Exception {
    String tableName = "solr4";

    shell.execute(createTableCommand(tableName, "id string, col1 array<array<int>>", true));
    shell.execute("insert into " + tableName + " select 'id0', array(array(12, 34, 56)) from temporary_table;");
    shell.execute("insert into " + tableName + " select 'id1', array(array(22, 33, 44)) from temporary_table;");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");

    try {
      QueryResponse queryResponse = solrClient.query(DEFAULT_COLLECTION, params);
      SolrDocumentList solrDocumentList = queryResponse.getResults();
      assertEquals(2, solrDocumentList.size());

      params = new ModifiableSolrParams();
      params.add("q", "id:\"id0\"");
      queryResponse = solrClient.query(DEFAULT_COLLECTION, params);
      solrDocumentList = queryResponse.getResults();
      assertEquals(1, solrDocumentList.size());
      SolrDocument solrDocument = solrDocumentList.get(0);

      assertEquals(Arrays.asList(12, 34, 56), solrDocument.get("col1.0.item_is"));
    } catch (SolrServerException e) {
      throw e;
    } catch (IOException e) {
      throw e;
    }
  }

  @Test
  public void testDataInsertArrayWithNestedMapField() throws Exception {
    String tableName = "solr5";

    shell.execute(createTableCommand(tableName, "id string, col1 array<map<int, string>>", true));
    shell.execute("insert into " + tableName + " select 'id0', array(map(1, 'abc', 2, 'def'), map(3, 'xyz', 4, "
      + "'wer')) from temporary_table;");
    shell.execute("insert into " + tableName + " select 'id1', array(map(10, 'asd', 20, 'fgh'), map(30, 'jkl', 40, "
      + "'ert')) from temporary_table;");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");

    try {
      QueryResponse queryResponse = solrClient.query(DEFAULT_COLLECTION, params);
      SolrDocumentList solrDocumentList = queryResponse.getResults();
      assertEquals(2, solrDocumentList.size());

      params = new ModifiableSolrParams();
      params.add("q", "id:\"id1\"");
      queryResponse = solrClient.query(DEFAULT_COLLECTION, params);
      solrDocumentList = queryResponse.getResults();
      assertEquals(1, solrDocumentList.size());
      SolrDocument solrDocument = solrDocumentList.get(0);

      assertEquals("asd", solrDocument.get("col1.0.10_s"));
      assertEquals("fgh", solrDocument.get("col1.0.20_s"));
      assertEquals("jkl", solrDocument.get("col1.1.30_s"));
      assertEquals("ert", solrDocument.get("col1.1.40_s"));
    } catch (SolrServerException e) {
      throw e;
    } catch (IOException e) {
      throw e;
    }
  }

  @Test
  public void testDataInsertMapWithPrimitiveFields() throws Exception {
    String tableName = "solr6";

    shell.execute(createTableCommand(tableName, "id string, col1 map<string, string>", true));
    shell.execute("insert into " + tableName + " select 'id0', map('xyz', 'abc', 'zxc', 'def', 'asd', 'dfg') from "
      + "temporary_table;");
    shell.execute("insert into " + tableName + " select 'id1', map('qwe', 'dfg', 'hjk', 'red', 'fds', 'nbv') from "
      + "temporary_table;");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");

    try {
      QueryResponse queryResponse = solrClient.query(DEFAULT_COLLECTION, params);
      SolrDocumentList solrDocumentList = queryResponse.getResults();
      assertEquals(2, solrDocumentList.size());

      params = new ModifiableSolrParams();
      params.add("q", "id:\"id0\"");
      queryResponse = solrClient.query(DEFAULT_COLLECTION, params);
      solrDocumentList = queryResponse.getResults();
      assertEquals(1, solrDocumentList.size());
      SolrDocument solrDocument = solrDocumentList.get(0);

      assertEquals("abc", solrDocument.get("col1.xyz_s"));
      assertEquals("def", solrDocument.get("col1.zxc_s"));
      assertEquals("dfg", solrDocument.get("col1.asd_s"));
    } catch (SolrServerException e) {
      throw e;
    } catch (IOException e) {
      throw e;
    }
  }

  @Test
  public void testDataInsertMapWithNestedMapField() throws Exception {
    String tableName = "solr7";

    shell.execute(createTableCommand(tableName, "id string, col1 map<string, map<int, string>>", true));
    shell.execute("insert into " + tableName + " select 'id0', map('efb', map(10, 'thn'), 'rfn', map(20, 'thk')) "
      + "from temporary_table;");
    shell.execute("insert into " + tableName + " select 'id1', map('xyz', map(30, 'tre'), 'def', map(40, 'jhg')) "
      + "from temporary_table;");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");

    try {
      QueryResponse queryResponse = solrClient.query(DEFAULT_COLLECTION, params);
      SolrDocumentList solrDocumentList = queryResponse.getResults();
      assertEquals(2, solrDocumentList.size());

      params = new ModifiableSolrParams();
      params.add("q", "id:\"id1\"");
      queryResponse = solrClient.query(DEFAULT_COLLECTION, params);
      solrDocumentList = queryResponse.getResults();
      assertEquals(1, solrDocumentList.size());
      SolrDocument solrDocument = solrDocumentList.get(0);

      assertEquals("tre", solrDocument.get("col1.xyz.30_s"));
      assertEquals("jhg", solrDocument.get("col1.def.40_s"));
    } catch (SolrServerException e) {
      throw e;
    } catch (IOException e) {
      throw e;
    }
  }

  @Test
  public void testDataInsertMapWithNestedListField() throws Exception {
    String tableName = "solr8";

    shell.execute(createTableCommand(tableName, "id string, col1 map<string, array<string>>", true));
    shell.execute("insert into " + tableName + " select 'id0', map('wsc', array('ytr', 'zxc'), 'yui', array('cde', "
      + "'dfg')) from temporary_table;");
    shell.execute("insert into " + tableName + " select 'id1', map('tgh', array('kjh', 'tre'), 'jko', array('nbv', "
      + "'jhg')) from temporary_table;");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");

    try {
      QueryResponse queryResponse = solrClient.query(DEFAULT_COLLECTION, params);
      SolrDocumentList solrDocumentList = queryResponse.getResults();
      assertEquals(2, solrDocumentList.size());

      params = new ModifiableSolrParams();
      params.add("q", "id:\"id0\"");
      queryResponse = solrClient.query(DEFAULT_COLLECTION, params);
      solrDocumentList = queryResponse.getResults();
      assertEquals(1, solrDocumentList.size());
      SolrDocument solrDocument = solrDocumentList.get(0);

      assertEquals(Arrays.asList("ytr", "zxc"), solrDocument.get("col1.wsc_ss"));
      assertEquals(Arrays.asList("cde", "dfg"), solrDocument.get("col1.yui_ss"));
    } catch (SolrServerException e) {
      throw e;
    } catch (IOException e) {
      throw e;
    }
  }
}
