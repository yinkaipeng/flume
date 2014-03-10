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

import com.google.common.collect.Lists;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.SimpleEvent;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.UUID;

public class TestHiveSink {
  final static String dbName = "testing";
  final static String tblName = "alerts";

  public static final String PART1_NAME = "continent";
  public static final String PART2_NAME = "country";
  public static final String[] partNames = { PART1_NAME, PART2_NAME };

  private static final String COL1 = "id";
  private static final String COL2 = "msg";
  final String[] colNames = {COL1,COL2};
  private String[] colTypes = { "int", "string" };

  private static final String PART1_VALUE = "Asia";
  private static final String PART2_VALUE = "India";
  private final ArrayList<String> partitionVals;


  HiveSink sink = new HiveSink();

  private final Driver driver;
  private final HiveConf conf;


  private final int port ;
  final String metaStoreURI ;


  private static final Logger LOG = LoggerFactory.getLogger(HiveSink.class);

  public TestHiveSink() throws Exception {
    partitionVals = new ArrayList<String>(2);
    partitionVals.add(PART1_VALUE);
    partitionVals.add(PART2_VALUE);

//    port = 9083;
    port = TestUtil.findFreePort();
    metaStoreURI = "thrift://localhost:" + port;

    conf = new HiveConf(this.getClass());
    TestUtil.setConfValues(conf);
    conf.setVar(HiveConf.ConfVars.METASTOREURIS, metaStoreURI);

    // 1) Start Metastore on a diff thread
    TestUtil.startLocalMetaStore(port,conf);

    // 2) Setup Hive client
    SessionState.start(new CliSessionState(conf));
    driver = new Driver(conf);
  }


  @Before
  public void setUp() throws Exception {
    hiveCleanup();

    LOG.debug("Starting...");

    sink = new HiveSink();
    sink.setName("HiveSink-" + UUID.randomUUID().toString());

    TestUtil.createDbAndTable(conf, dbName, tblName, partitionVals, colNames, colTypes, partNames);
  }

  @After
  public void tearDown() throws MetaException, HiveException {
    hiveCleanup();
  }

  private void hiveCleanup() throws MetaException, HiveException {
    try {
      TestUtil.dropDB(conf, dbName);
    } finally {
//      msClient.close();
    }
  }



  @Test
  public void testDummy() throws EventDeliveryException {
    int batchSize = 2;
    Context context = new Context();
    context.put("hive.metastore",metaStoreURI);
    context.put("hive.database",dbName);
    context.put("hive.table",tblName);
    context.put("hive.partition", PART1_VALUE + "," + PART2_VALUE);
    context.put("autoCreatePartitions","true");
    context.put("batchSize","" + batchSize);
    context.put("serializer", HiveDelimitedTextSerializer.ALIAS);
    context.put("serializer.fieldnames", COL1 + ",," + COL2 + ",");

    Channel channel = startSink(sink, context);

//    Transaction tx = channel.getTransaction();
//    tx.begin();

    Calendar eventDate = Calendar.getInstance();
    List<String> bodies = Lists.newArrayList();
//
    // push the events in two batches
    for (int i = 0; i < 2; i++) {
      Transaction txn = channel.getTransaction();
      txn.begin();
      for (int j = 1; j <= batchSize; j++) {
        Event event = new SimpleEvent();
        eventDate.clear();
        eventDate.set(2011, 01, 1+i, 0, 0+j); // yy mm dd
        String body = i*j + ",blah,This is a log message,other stuff";
        event.setBody(body.getBytes());
        bodies.add(body);
        channel.put(event);
      }
      txn.commit();
      txn.close();

      // execute sink to process the events
      sink.process();
    }
    sink.stop();

  }

  private static Channel startSink(HiveSink sink, Context context) {
    Configurables.configure(sink, context);

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, context);
    sink.setChannel(channel);
    sink.start();
    return channel;
  }
}

//TODO: Serializer needs to discard extra fields in the end
// tests: for
// - add partition
// - pattern substitution
// - time rounding etc
// - expiration
// - counters
// - reconfigure
// - multiple writers