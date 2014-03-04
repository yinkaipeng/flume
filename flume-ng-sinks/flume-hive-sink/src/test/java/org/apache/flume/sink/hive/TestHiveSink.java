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
import junit.framework.Assert;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
    TestUtil.startLocalMetaStore(port, conf);

    // 2) Setup Hive client
    SessionState.start(new CliSessionState(conf));
    driver = new Driver(conf);
  }


  @Before
  public void setUp() throws Exception {
    TestUtil.dropDB(conf, dbName);

    LOG.debug("Starting...");

    sink = new HiveSink();
    sink.setName("HiveSink-" + UUID.randomUUID().toString());

    TestUtil.createDbAndTable(conf, dbName, tblName, partitionVals, colNames,
            colTypes, partNames);
  }

  @After
  public void tearDown() throws MetaException, HiveException {
    TestUtil.dropDB(conf, dbName);
  }


  @Test
  public void testSingleWriterSimple()
          throws EventDeliveryException, IOException, CommandNeedRetryException {
    int batchSize = 2;
    Context context = new Context();
    context.put("hive.metastore",metaStoreURI);
    context.put("hive.database",dbName);
    context.put("hive.table",tblName);
    context.put("hive.partition", PART1_VALUE + "," + PART2_VALUE);
    context.put("autoCreatePartitions","false");
    context.put("batchSize","" + batchSize);
    context.put("serializer", HiveDelimitedTextSerializer.ALIAS);
    context.put("serializer.fieldnames", COL1 + ",," + COL2 + ",");

    Channel channel = startSink(sink, context);

//    Calendar eventDate = Calendar.getInstance();
    List<String> bodies = Lists.newArrayList();

    // push the events in two batches
    for (int i = 0; i < 2; i++) {
      Transaction txn = channel.getTransaction();
      txn.begin();
      for (int j = 1; j <= batchSize; j++) {
        Event event = new SimpleEvent();
//        eventDate.clear();
//        eventDate.set(2011, 01, 1+i, 0, 0+j); // yy mm dd
        String body = i*j + ",blah,This is a log message,other stuff";
        event.setBody(body.getBytes());
        bodies.add(body);
        channel.put(event);
      }
      // execute sink to process the events
      txn.commit();
      txn.close();

      checkRecordCountInTable(0);
      sink.process();
      checkRecordCountInTable(4);
    }
    sink.stop();
    checkRecordCountInTable(4);
  }

  @Test
  public void testSingleWriterUseHeaders()
          throws Exception {
    String[] colNames = {COL1, COL2};
    String PART1_NAME = "country";
    String PART2_NAME = "hour";
    String[] partNames = {PART1_NAME, PART2_NAME};
    List<String> partitionVals = null;
    String PART1_VALUE = "%{" + PART1_NAME + "}";
    String PART2_VALUE = "%y-%m-%d-%k";

    String tblName = "hourlydata";
    TestUtil.dropDB(conf, dbName);
    TestUtil.createDbAndTable(conf, dbName, tblName, partitionVals, colNames,
            colTypes, partNames);

    int batchSize = 2;
    Context context = new Context();
    context.put("hive.metastore",metaStoreURI);
    context.put("hive.database",dbName);
    context.put("hive.table",tblName);
    context.put("hive.partition", PART1_VALUE + "," + PART2_VALUE);
    context.put("autoCreatePartitions","true");
    context.put("useLocalTimeStamp", "false");
    context.put("batchSize","" + batchSize);
    context.put("serializer", HiveDelimitedTextSerializer.ALIAS);
    context.put("serializer.fieldnames", COL1 + ",," + COL2 + ",");

    Channel channel = startSink(sink, context);

    Calendar eventDate = Calendar.getInstance();
    List<String> bodies = Lists.newArrayList();

    // push events in two batches - two per batch. each batch is diff hour
    for (int i = 0; i < 2; i++) {
      Transaction txn = channel.getTransaction();
      txn.begin();
      for (int j = 1; j <= batchSize; j++) {
        Event event = new SimpleEvent();
        String body = i*j + ",blah,This is a log message,other stuff";
        event.setBody(body.getBytes());
        eventDate.clear();
        eventDate.set(2014, 03, 03, i, 1); // yy mm dd hh mm
        event.getHeaders().put( "timestamp",
                String.valueOf(eventDate.getTimeInMillis()) );
        event.getHeaders().put( PART1_NAME, "Asia" );
        bodies.add(body);
        channel.put(event);
      }
      // execute sink to process the events
      txn.commit();
      txn.close();

//      checkRecordCountInTable(0);
      sink.process();
//      checkRecordCountInTable(4);
    }
    // verify counters
    SinkCounter counter = sink.getCounter();
    Assert.assertEquals(2, counter.getConnectionCreatedCount() );
    Assert.assertEquals(0, counter.getConnectionClosedCount());

    // stop sink and verify data
    sink.stop();
    checkRecordCountInTable(4);

    // verify counters
    Assert.assertEquals(2, counter.getConnectionCreatedCount());
    Assert.assertEquals(2, counter.getConnectionClosedCount());
    Assert.assertEquals(2, counter.getBatchCompleteCount());
    Assert.assertEquals(0, counter.getBatchEmptyCount());
    Assert.assertEquals(0, counter.getConnectionFailedCount() );
    Assert.assertEquals(4, counter.getEventDrainAttemptCount());
    Assert.assertEquals(4, counter.getEventDrainSuccessCount() );

  }

  private static Channel startSink(HiveSink sink, Context context) {
    Configurables.configure(sink, context);

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, context);
    sink.setChannel(channel);
    sink.start();
    return channel;
  }

  private void checkRecordCountInTable(int expectedCount)
          throws CommandNeedRetryException, IOException {
    int count = TestUtil.listRecordsInTable(driver, dbName, tblName).size();
    Assert.assertEquals(expectedCount, count);
  }
}

//TODO: Serializer needs to discard/ignore extra fields in the end
// tests: for
// + add partition
// + pattern substitution (headers & ts)
// + counters
// + multiple writers
// + use local time
// +
// - time rounding etc
// - expiration
// - reconfigure
