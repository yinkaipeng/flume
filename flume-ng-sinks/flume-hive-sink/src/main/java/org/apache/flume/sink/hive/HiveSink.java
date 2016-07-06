/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.sink.hive;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.SystemClock;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.BucketPath;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.hcatalog.streaming.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class HiveSink extends AbstractSink implements Configurable {

  private static final Logger LOG = LoggerFactory
      .getLogger(HiveSink.class);

  private static final int defaultMaxOpenConnections = 500;
  private static final int defaultTxnsPerBatch = 100;
  private static final int defaultBatchSize = 15000;
  private static final int defaultCallTimeout = 10000;
  private static final int defaultIdleTimeout = 0;
  private static final int defautHeartBeatInterval = 240; // seconds



  HashMap<HiveEndPoint, HiveWriter> allWriters;

  private SinkCounter sinkCounter;
  private volatile int idleTimeout;
  private String metaStoreUri;
  private String kerberosPrincipal;
  private String kerberosKeytab;
  private String proxyUser;
  private String database;
  private String table;
  private List<String> partitionVals;
  private Integer txnsPerBatchAsk;
  private Integer batchSize;
  private Integer maxOpenConnections;
  private boolean autoCreatePartitions;
  private String serializerType;

  HiveEventSerializer serializer;
  private boolean kerberosEnabled;
  private UserGroupInformation ugi;

  /**
   * Default timeout for blocking I/O calls in HiveWriter
   */
  private Integer callTimeout;
  private Integer heartBeatInterval;

  private ExecutorService callTimeoutPool;

//  private SystemClock clock;
  private boolean useLocalTime;
  private TimeZone timeZone;
  private boolean needRounding;
  private int roundUnit;
  private Integer roundValue;
  private SystemClock clock;
  Timer heartBeatTimer = new Timer();
  private AtomicBoolean timeToSendHeartBeat = new AtomicBoolean(false);

  @VisibleForTesting
  HashMap<HiveEndPoint, HiveWriter> getAllWriters() {
    return allWriters;
  }

  // read configuration and setup thresholds
  @Override
  public void configure(Context context) {

    // - METASTORE URI conf
    metaStoreUri = context.getString("hive.metastore");
    if(metaStoreUri==null) {
      throw new IllegalArgumentException("hive.metastore config setting is not " +
              "specified for sink " + getName());
    }
    if (metaStoreUri.equalsIgnoreCase("null")) { // for testing support
      metaStoreUri = null;
    }
    proxyUser = null; // context.getString("hive.proxyUser"); not supported by hive api yet

    // - Kerberos Authentication conf
    String oldKerberosPrincipal = kerberosPrincipal;
    kerberosPrincipal = context.getString("hive.kerberosPrincipal", null);
    String oldKerberosKeytab = kerberosKeytab;
    kerberosKeytab = context.getString("hive.kerberosKeytab", null);

    if(kerberosKeytab==null && kerberosPrincipal==null) {
      kerberosEnabled = false;
    } else if ( kerberosPrincipal!=null && kerberosKeytab!=null ) {
      kerberosEnabled = true;
    } else {
      throw new IllegalArgumentException("To enable Kerberos, need to set both " +
              "hive.kerberosPrincipal & hive.kerberosKeytab. Cannot set just one of them. "
              + getName());
    }

    boolean needReauth = hasCredentialChanged(oldKerberosKeytab, oldKerberosPrincipal
            , kerberosKeytab, kerberosPrincipal);
    if( needReauth ) {
      if (kerberosEnabled) {
        try {
          ugi = authenticate(kerberosKeytab, kerberosPrincipal);
        } catch (AuthenticationFailed ex) {
          LOG.error(getName() + " : " + ex.getMessage(), ex);
          throw new IllegalArgumentException(ex);
        }
      } else {
        ugi = null;
      }
    }

    // - Table/Partition conf
    database = context.getString("hive.database");
    if(database==null) {
      throw new IllegalArgumentException("hive.database config setting is not " +
            "specified for sink " + getName());
    }
    table = context.getString("hive.table");
    if(table==null) {
      throw new IllegalArgumentException("hive.table config setting is not " +
              "specified for sink " + getName());
    }

    String partitions = context.getString("hive.partition");
    if(partitions!=null) {
      partitionVals = Arrays.asList(partitions.split(","));
    }

    // - Hive Txn size
    txnsPerBatchAsk = context.getInteger("hive.txnsPerBatchAsk", defaultTxnsPerBatch);
    if(txnsPerBatchAsk<0) {
      LOG.warn(getName() + ". hive.txnsPerBatchAsk must be  positive number. Defaulting to " + defaultTxnsPerBatch);
      txnsPerBatchAsk = defaultTxnsPerBatch;
    }

    // - Other Sink parameters
    batchSize = context.getInteger("batchSize", defaultBatchSize);
    if(batchSize<0) {
      LOG.warn(getName() + ". batchSize must be  positive number. Defaulting to " + defaultBatchSize);
      batchSize = defaultBatchSize;
    }
    idleTimeout = context.getInteger("idleTimeout", defaultIdleTimeout);
    if(idleTimeout<0) {
      LOG.warn(getName() + ". idleTimeout must be  positive number. Defaulting to " + defaultIdleTimeout);
      idleTimeout = defaultIdleTimeout;
    }
    callTimeout = context.getInteger("callTimeout", defaultCallTimeout);
    if(callTimeout<0) {
      LOG.warn(getName() + ". callTimeout must be  positive number. Defaulting to " + defaultCallTimeout);
      callTimeout = defaultCallTimeout;
    }

    heartBeatInterval = context.getInteger("heartBeatInterval", defautHeartBeatInterval);
    if(heartBeatInterval<0) {
      LOG.warn(getName() + ". heartBeatInterval must be  positive number. Defaulting to " + defautHeartBeatInterval);
      heartBeatInterval = defautHeartBeatInterval;
    }
    maxOpenConnections = context.getInteger("maxOpenConnections", defaultMaxOpenConnections);
    autoCreatePartitions =  context.getBoolean("autoCreatePartitions", true);

    // Timestamp processing
    useLocalTime = context.getBoolean("useLocalTimeStamp", false);
    if(useLocalTime) {
      clock = new SystemClock();
    }
    String tzName = context.getString("timeZone");
    timeZone = (tzName == null) ? null : TimeZone.getTimeZone(tzName);
    needRounding = context.getBoolean("round", false);

    String unit = context.getString("roundUnit", "minute");
    if (unit.equalsIgnoreCase("hour")) {
      this.roundUnit = Calendar.HOUR_OF_DAY;
    } else if (unit.equalsIgnoreCase("minute")) {
      this.roundUnit = Calendar.MINUTE;
    } else if (unit.equalsIgnoreCase("second")){
      this.roundUnit = Calendar.SECOND;
    } else {
      LOG.warn(getName() + ". Rounding unit is not valid, please set one of " +
              "minute, hour or second. Rounding will be disabled");
      needRounding = false;
    }
    this.roundValue = context.getInteger("roundValue", 1);
    if(roundUnit == Calendar.SECOND || roundUnit == Calendar.MINUTE){
      Preconditions.checkArgument(roundValue > 0 && roundValue <= 60,
              "Round value must be > 0 and <= 60");
    } else if (roundUnit == Calendar.HOUR_OF_DAY){
      Preconditions.checkArgument(roundValue > 0 && roundValue <= 24,
              "Round value must be > 0 and <= 24");
    }

    // Serializer
    serializerType = context.getString("serializer");
    if(serializerType == null) {
      throw new IllegalArgumentException("serializer config setting is not " +
              "specified for sink " + getName());
    }

    serializer = createSerializer(serializerType);
    serializer.configure(context);

//    kerbConfPrincipal = context.getString("hdfs.kerberosPrincipal", "");
//    kerbKeytab = context.getString("hdfs.kerberosKeytab", "");

    Preconditions.checkArgument(batchSize > 0, "batchSize must be greater than 0");

    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName());
    }
  }

  private static boolean hasCredentialChanged(String oldKerberosKeytab, String oldKerberosPrincipal,
                                            String newKerberosKeytab, String newKerberosPrincipal) {
    // see of keytab changed
    if ( hasChanged(oldKerberosKeytab, newKerberosKeytab) )
      return true;

    // see of principal changed
    if ( hasChanged(oldKerberosPrincipal, newKerberosPrincipal) )
      return true;

      return false;
  }

  private static boolean hasChanged(String oldVal, String newVal) {
    if( oldVal == null) {
       if ( newVal != null ) {
         return true;
       }
      return false;
    } else {
      if (newVal == null) {
        return true;
      }
    }
    if( oldVal.compareTo(newVal) != 0) {
      return true;
    }
    return false;
  }

  // Synchronized to ensure atomic loginUserFromKeytab() + getLoginUser()
  private static synchronized UserGroupInformation authenticate(String keytab, String principal)
          throws AuthenticationFailed {
    // 0) Check keytab file is readable
    File kfile = new File(keytab);
    if (!(kfile.isFile() && kfile.canRead())) {
      throw new IllegalArgumentException("The keyTab file: "
              + keytab + " is nonexistent or can't read. "
              + "Please specify a readable keytab file for Kerberos auth.");
    }

    // 1) resolve _HOST pattern in principal via DNS (as 2nd argument is empty)
    try {
      principal = SecurityUtil.getServerPrincipal(principal, "");
      Preconditions.checkNotNull(principal, "Principal resolved to null");
    } catch (Exception e) {
      throw new AuthenticationFailed("Host lookup error when resolving principal " + principal, e);
    }

    // 2) Login with principal & keytab
    try {
      UserGroupInformation.loginUserFromKeytab(principal, keytab);
      return UserGroupInformation.getLoginUser();
    } catch (IOException e) {
      throw new AuthenticationFailed("Login failed for principal " + principal, e);
    }
  }


  @VisibleForTesting
  protected SinkCounter getCounter() {
    return sinkCounter;
  }
  private HiveEventSerializer createSerializer(String serializerName)  {
    if(serializerName.compareToIgnoreCase(HiveDelimitedTextSerializer.ALIAS)==0 ||
            serializerName.compareTo(HiveDelimitedTextSerializer.class.getName())==0) {
      return new HiveDelimitedTextSerializer();
    } else if (serializerName.compareToIgnoreCase(HiveJsonSerializer.ALIAS)==0 ||
            serializerName.compareTo(HiveJsonSerializer.class.getName())==0) {
      return new HiveJsonSerializer();
    }

    try {
      return (HiveEventSerializer) Class.forName(serializerName).newInstance();
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to instantiate serializer: " + serializerName + " on sink: " + getName(), e);
    }
  }


  /**
   * Pull events out of channel, find corresponding HiveWriter and write to it.
   * Take at most batchSize events per Transaction. <br/>
   * This method is not thread safe.
   */
  public Status process() throws EventDeliveryException {
    // writers used in this Txn

    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();
    transaction.begin();
    boolean success = false;
    try {
      // 1 Enable Heart Beats
      if(timeToSendHeartBeat.compareAndSet(true, false)) {
        enableHeartBeatOnAllWriters();
      }

      // 2 Drain Batch
      int txnEventCount = drainOneBatch(channel);
      transaction.commit();
      success = true;

      // 3 Update Counters
      if (txnEventCount < 1) {
        return Status.BACKOFF;
      } else {
        return Status.READY;
      }
    } catch (InterruptedException err) {
      LOG.warn(getName() + ": Thread was interrupted.", err);
      return Status.BACKOFF;
    } catch (Exception e) {
      throw new EventDeliveryException(e);
    } finally {
      if(!success) {
        transaction.rollback();
      }
      transaction.close();
    }
  }

  // Drains one batch of events from Channel into Hive
  private int drainOneBatch(Channel channel)
          throws HiveWriter.Failure, InterruptedException {
    int txnEventCount = 0;
    try {
      HashMap<HiveEndPoint,HiveWriter> activeWriters = Maps.newHashMap();
      for (; txnEventCount < batchSize; ++txnEventCount) {
        // 0) Read event from Channel
        Event event = channel.take();
        if (event == null) {
          break;
        }

        //1) Create end point by substituting place holders
        HiveEndPoint endPoint = makeEndPoint(metaStoreUri, database, table,
                partitionVals, event.getHeaders(), timeZone,
                needRounding, roundUnit, roundValue, useLocalTime);

        //2) Create or reuse Writer
        HiveWriter writer = getOrCreateWriter(activeWriters, endPoint);

        //3) Write
        LOG.debug("{} : Writing event to {}", getName(), endPoint);
        writer.write(event);

      } // for

      //4) Update counters
      if (txnEventCount == 0) {
        sinkCounter.incrementBatchEmptyCount();
      } else if (txnEventCount == batchSize) {
        sinkCounter.incrementBatchCompleteCount();
      } else {
        sinkCounter.incrementBatchUnderflowCount();
      }
      sinkCounter.addToEventDrainAttemptCount(txnEventCount);


      // 5) Flush all Writers
      for (HiveWriter writer : activeWriters.values()) {
        writer.flush(true);
      }

      sinkCounter.addToEventDrainSuccessCount(txnEventCount);
      return txnEventCount;
    } catch (HiveWriter.Failure e) {
      // in case of error we close all TxnBatches to start clean next time
      LOG.warn(getName() + " : " + e.getMessage(), e);
      abortAllWriters();
      closeAllWriters();
      throw e;
    }
  }

  private void enableHeartBeatOnAllWriters() {
    for (HiveWriter writer : allWriters.values()) {
      writer.setHearbeatNeeded();
    }
  }

  private HiveWriter getOrCreateWriter(HashMap<HiveEndPoint, HiveWriter> activeWriters,
                                       HiveEndPoint endPoint)
          throws HiveWriter.ConnectFailure, InterruptedException {
    try {
      HiveWriter writer = allWriters.get( endPoint );
      if( writer == null ) {
        LOG.info(getName() + ": Creating Writer to Hive end point : " + endPoint);
        writer = new HiveWriter(endPoint, txnsPerBatchAsk, autoCreatePartitions,
                callTimeout, callTimeoutPool, ugi, serializer, sinkCounter);

        sinkCounter.incrementConnectionCreatedCount();
        if(allWriters.size() > maxOpenConnections){
          int retired = closeIdleWriters();
          if(retired==0) {
            closeEldestWriter();
          }
        }
        allWriters.put(endPoint, writer);
        activeWriters.put(endPoint, writer);
      }
      else {
        if(activeWriters.get(endPoint)==null)  {
          activeWriters.put(endPoint,writer);
        }
      }
      return writer;
    } catch (HiveWriter.ConnectFailure e) {
      sinkCounter.incrementConnectionFailedCount();
      throw e;
    }

  }

  private HiveEndPoint makeEndPoint(String metaStoreUri, String database, String table,
                                    List<String> partVals, Map<String, String> headers,
                                    TimeZone timeZone, boolean needRounding,
                                    int roundUnit, Integer roundValue,
                                    boolean useLocalTime)  {
    if(partVals==null) {
      return new HiveEndPoint(metaStoreUri, database, table, null);
    }

    ArrayList<String> realPartVals = Lists.newArrayList();
    for(String partVal : partVals) {
      realPartVals.add(BucketPath.escapeString(partVal, headers, timeZone,
              needRounding, roundUnit, roundValue, useLocalTime));
    }
    return new HiveEndPoint(metaStoreUri, database, table, realPartVals);
  }

  /**
   * Locate writer that has not been used for longest time and retire it
   */
  private void closeEldestWriter() throws InterruptedException {
    long oldestTimeStamp = System.currentTimeMillis();
    HiveEndPoint eldest = null;
    for (Entry<HiveEndPoint,HiveWriter> entry : allWriters.entrySet()) {
      if(entry.getValue().getLastUsed() < oldestTimeStamp) {
        eldest = entry.getKey();
        oldestTimeStamp = entry.getValue().getLastUsed();
      }
    }

    try {
      sinkCounter.incrementConnectionCreatedCount();
      LOG.info(getName() + ": Closing least used Writer to Hive EndPoint : " + eldest);
      allWriters.remove(eldest).close();
    } catch (InterruptedException e) {
      LOG.warn(getName() + ": Interrupted when attempting to close writer for end point: " + eldest, e);
      throw e;
    }
  }

  /**
   * Locate all writers past idle timeout and retire them
   * @return number of writers retired
   */
  private int closeIdleWriters() throws InterruptedException {
    int count = 0;
    long now = System.currentTimeMillis();
    ArrayList<HiveEndPoint> retirees = Lists.newArrayList();

    //1) Find retirement candidates
    for (Entry<HiveEndPoint,HiveWriter> entry : allWriters.entrySet()) {
      if(now - entry.getValue().getLastUsed() > idleTimeout) {
        ++count;
        retirees.add(entry.getKey());
      }
    }
    //2) Retire them
    for(HiveEndPoint ep : retirees) {
      sinkCounter.incrementConnectionClosedCount();
      LOG.info(getName() + ": Closing idle Writer to Hive end point : {}", ep);
      allWriters.remove(ep).close();
    }
    return count;
  }

  /**
   * Closes all writers and remove them from cache
   * @return number of writers retired
   */
  private void closeAllWriters() throws InterruptedException {
    //1) Retire writers
    for (Entry<HiveEndPoint,HiveWriter> entry : allWriters.entrySet()) {
        entry.getValue().close();
    }

    //2) Clear cache
    allWriters.clear();
  }

  /**
   * Abort current Txn on all writers
   * @return number of writers retired
   */
  private void abortAllWriters() throws InterruptedException {
    for (Entry<HiveEndPoint,HiveWriter> entry : allWriters.entrySet()) {
        entry.getValue().abort();
    }
  }

  @Override
  public void stop() {
    // do not constrain close() calls with a timeout
    for (Entry<HiveEndPoint, HiveWriter> entry : allWriters.entrySet()) {
      try {
        HiveWriter w = entry.getValue();
        w.abort();
        w.close();
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }

    // shut down all thread pools
    ExecutorService toShutdown[] = {callTimeoutPool};
    for (ExecutorService execService : toShutdown) {
      execService.shutdown();
      try {
        while (execService.isTerminated() == false) {
          execService.awaitTermination(
                Math.max(defaultCallTimeout, callTimeout), TimeUnit.MILLISECONDS);
        }
      } catch (InterruptedException ex) {
        LOG.warn(getName() + ":Shutdown interrupted on " + execService, ex);
      }
    }

    callTimeoutPool = null;
    allWriters.clear();
    allWriters = null;
    sinkCounter.stop();
    super.stop();
    LOG.info("Hive Sink {} stopped", getName() );
  }

  @Override
  public void start() {
    String timeoutName = "hive-" + getName() + "-call-runner-%d";
    // call timeout pool needs only 1 thd as sink is effectively single threaded
    callTimeoutPool = Executors.newFixedThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat(timeoutName).build());

    this.allWriters = Maps.newHashMap();
    sinkCounter.start();
    super.start();
    setupHeartBeatTimer();
    LOG.info(getName() + ": Hive Sink {} started", getName() );
  }

  private void setupHeartBeatTimer() {
    if(heartBeatInterval>0) {
      heartBeatTimer.schedule(new TimerTask() {
        @Override
        public void run() {
          timeToSendHeartBeat.set(true);
          setupHeartBeatTimer();
        }
      }, heartBeatInterval * 1000);
    }
  }


  @Override
  public String toString() {
    return "{ Sink type:" + getClass().getSimpleName() + ", name:" + getName() +
            " }";
  }

  private static class AuthenticationFailed extends Exception {
    public AuthenticationFailed(String reason, Exception cause) {
      super("Kerberos Authentication Failed. " + reason, cause);
    }
  }
}
