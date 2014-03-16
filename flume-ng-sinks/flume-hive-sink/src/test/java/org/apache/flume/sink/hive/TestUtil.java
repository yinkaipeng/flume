/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.apache.flume.sink.hive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestUtil {

  private final static String txnMgr = "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager";

  /**
   * Set up the configuration so it will use the DbTxnManager, concurrency will be set to true,
   * and the JDBC configs will be set for putting the transaction and lock info in the embedded
   * metastore.
   * @param conf HiveConf to add these values to.
   */
  public static void setConfValues(HiveConf conf) {
    conf.setVar(HiveConf.ConfVars.HIVE_TXN_MANAGER, txnMgr);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
  }

  public static void createDbAndTable(HiveConf conf, String databaseName,
                                      String tableName, List<String> partVals,
                                      String[] colNames, String[] colTypes,
                                      String[] partNames)
          throws Exception {
    Hive hive = Hive.get(conf);
    IMetaStoreClient client = hive.getMSC();

    try {
      Database db = new Database();
      db.setName(databaseName);
      client.createDatabase(db);

      Table tbl = new Table();
      tbl.setDbName(databaseName);
      tbl.setTableName(tableName);
      tbl.setTableType(TableType.MANAGED_TABLE.toString());
      StorageDescriptor sd = new StorageDescriptor();
      sd.setCols(getTableColumns(colNames, colTypes));
      sd.setNumBuckets(10);
      if(partVals!=null && !partVals.isEmpty()) {
        tbl.setPartitionKeys(getPartitionKeys(partNames));
      }

      tbl.setSd(sd);

      sd.setBucketCols(new ArrayList<String>(2));
      sd.setSerdeInfo(new SerDeInfo());
      sd.getSerdeInfo().setName(tbl.getTableName());
      sd.getSerdeInfo().setParameters(new HashMap<String, String>());
      sd.getSerdeInfo().getParameters().put(serdeConstants.SERIALIZATION_FORMAT, "1");

      sd.getSerdeInfo().setSerializationLib(OrcSerde.class.getName());
      sd.setInputFormat(OrcInputFormat.class.getName());
      sd.setOutputFormat(OrcOutputFormat.class.getName());

      Map<String, String> tableParams = new HashMap<String, String>();
      tbl.setParameters(tableParams);
      client.createTable(tbl);

      if(partVals!=null && partVals.size() > 0) {
        addPartition(client, tbl, partVals);
      }
    } finally {
//      client.close();
    }
  }

  // delete db and all tables in it
  public static void dropDB(HiveConf conf, String databaseName) throws HiveException, MetaException {
    Hive hive = Hive.get(conf);
    IMetaStoreClient client = hive.getMSC();
    try {
      for(String table : client.listTableNamesByFilter(databaseName, "", (short)-1)) {
        client.dropTable(databaseName, table, true, true);
      }
      client.dropDatabase(databaseName);
    } catch (TException e) {
    }
  }

  private static void addPartition(IMetaStoreClient client, Table tbl
          , List<String> partValues)
          throws IOException, TException {
    Partition part = new Partition();
    part.setDbName(tbl.getDbName());
    part.setTableName(tbl.getTableName());
    part.setSd(tbl.getSd());
    part.setValues(partValues);
    client.add_partition(part);
  }

  private static List<FieldSchema> getTableColumns(String[] colNames, String[] colTypes) {
    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    for (int i=0; i<colNames.length; ++i) {
      fields.add(new FieldSchema(colNames[i], colTypes[i], ""));
    }
    return fields;
  }

  private static List<FieldSchema> getPartitionKeys(String[] partNames) {
    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    for (int i=0; i<partNames.length; ++i) {
      fields.add(new FieldSchema(partNames[i], serdeConstants.STRING_TYPE_NAME, ""));
    }
    return fields;
  }

  public static int findFreePort() throws IOException {
    return  MetaStoreUtils.findFreePort();
  }

  public static void startLocalMetaStore(int port, HiveConf conf) throws Exception {
    TxnDbUtil.cleanDb();
    TxnDbUtil.prepDb();
    MetaStoreUtils.startMetaStore(port, ShimLoader.getHadoopThriftAuthBridge(), conf);
  }

  public static ArrayList<String> listRecordsInTable(Driver driver, String dbName, String tblName)
          throws CommandNeedRetryException, IOException {
    driver.run("select * from " + dbName + "." + tblName);
    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
    return res;
  }

  public static ArrayList<String> listRecordsInPartition(Driver driver, String dbName,
                               String tblName, String continent, String country)
          throws CommandNeedRetryException, IOException {
    driver.run("select * from " + dbName + "." + tblName + " where continent='"
            + continent + "' and country='" + country + "'");
    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
    return res;
  }


}
