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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.util.Shell;
import org.apache.thrift.TException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
    conf.set("fs.raw.impl", RawFileSystem.class.getName());
  }

  public static void createDbAndTable(HiveConf conf, String databaseName,
                                      String tableName, List<String> partVals,
                                      String[] colNames, String[] colTypes,
                                      String[] partNames, String dbLocation)
          throws Exception {
    IMetaStoreClient client = new HiveMetaStoreClient(conf);

    try {
      String dbUri = "raw://" + dbLocation;
              Database db = new Database();
      db.setName(databaseName);
      db.setLocationUri(dbUri);
      client.createDatabase(db);

      Table tbl = new Table();
      tbl.setDbName(databaseName);
      tbl.setTableName(tableName);
      tbl.setTableType(TableType.MANAGED_TABLE.toString());
      StorageDescriptor sd = new StorageDescriptor();
      sd.setCols(getTableColumns(colNames, colTypes));
      sd.setNumBuckets(10);
      sd.setLocation(dbUri + Path.SEPARATOR + tableName);
      if(partNames!=null && partNames.length!=0) {
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
      client.close();
    }
  }

  // delete db and all tables in it
  public static void dropDB(HiveConf conf, String databaseName) throws HiveException, MetaException {
    IMetaStoreClient client = new HiveMetaStoreClient(conf);
    try {
      for(String table : client.listTableNamesByFilter(databaseName, "", (short)-1)) {
        client.dropTable(databaseName, table, true, true);
      }
      client.dropDatabase(databaseName);
    } catch (TException e) {
      client.close();
    }
  }

  private static void addPartition(IMetaStoreClient client, Table tbl
          , List<String> partValues)
          throws IOException, TException {
    Partition part = new Partition();
    part.setDbName(tbl.getDbName());
    part.setTableName(tbl.getTableName());
    StorageDescriptor sd = new StorageDescriptor(tbl.getSd());
    sd.setLocation(sd.getLocation() + Path.SEPARATOR + makePartPath(tbl.getPartitionKeys(), partValues));
    part.setSd(sd);
    part.setValues(partValues);
    client.add_partition(part);
  }

  private static String makePartPath(List<FieldSchema> partKeys, List<String> partVals) {
    if(partKeys.size()!=partVals.size()) {
      throw new IllegalArgumentException("Partition values:" + partVals +
              ", does not match the partition Keys in table :" + partKeys );
    }
    StringBuffer buff = new StringBuffer(partKeys.size()*20);
    int i=0;
    for(FieldSchema schema : partKeys) {
      buff.append(schema.getName());
      buff.append("=");
      buff.append(partVals.get(i));
      if(i!=partKeys.size()-1) {
        buff.append(Path.SEPARATOR);
      }
      ++i;
    }
    return buff.toString();
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

//  public static int findFreePort() throws IOException {
//    return  MetaStoreUtils.findFreePort();
//  }
//
//  public static void startLocalMetaStore(int port, HiveConf conf) throws Exception {
//    TxnDbUtil.cleanDb();
//    TxnDbUtil.prepDb();
////    MetaStoreUtils.startMetaStore(port, ShimLoader.getHadoopThriftAuthBridge(), conf);
//  }

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


  public static class RawFileSystem extends RawLocalFileSystem {
    private static final URI NAME;
    static {
      try {
        NAME = new URI("raw:///");
      } catch (URISyntaxException se) {
        throw new IllegalArgumentException("bad uri", se);
      }
    }

    @Override
    public URI getUri() {
      return NAME;
    }

    static String execCommand(File f, String... cmd) throws IOException {
      String[] args = new String[cmd.length + 1];
      System.arraycopy(cmd, 0, args, 0, cmd.length);
      args[cmd.length] = f.getCanonicalPath();
      String output = Shell.execCommand(args);
      return output;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
      File file = pathToFile(path);
      if (!file.exists()) {
        throw new FileNotFoundException("Can't find " + path);
      }
      // get close enough
      short mod = 0;
      if (file.canRead()) {
        mod |= 0444;
      }
      if (file.canWrite()) {
        mod |= 0200;
      }
      if (file.canExecute()) {
        mod |= 0111;
      }
      ShimLoader.getHadoopShims();
      return new FileStatus(file.length(), file.isDirectory(), 1, 1024,
              file.lastModified(), file.lastModified(),
              FsPermission.createImmutable(mod), "owen", "users", path);
    }
  }

}
