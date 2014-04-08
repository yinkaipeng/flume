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
import org.apache.hive.hcatalog.streaming.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    metaStoreUri = context.getString("hive.metastore");
    if(metaStoreUri==null) {
      throw new IllegalArgumentException("hive.metastore config setting is not " +
              "specified for sink " + getName());
    }
    if (metaStoreUri.equalsIgnoreCase("null")) { // for testing support
      metaStoreUri = null;
    }
    proxyUser = null; // context.getString("hive.proxyUser"); not supported by hive api yet
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


    txnsPerBatchAsk = context.getInteger("hive.txnsPerBatchAsk", defaultTxnsPerBatch);
    if(txnsPerBatchAsk<0) {
      LOG.warn(getName() + ". hive.txnsPerBatchAsk must be  positive number. Defaulting to " + defaultTxnsPerBatch);
      txnsPerBatchAsk = defaultTxnsPerBatch;
    }
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

//    if (!authenticate()) {
//      LOG.error("Failed to authenticate!");
//    }

    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName());
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
    HashMap<HiveEndPoint,HiveWriter> activeWriters = Maps.newHashMap();

    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();
    if(timeToSendHeartBeat.compareAndSet(true, false)) {
      enableHeartBeatOnAllWriters();
    }
    transaction.begin();
    try {
      int txnEventCount = 0;
      for (; txnEventCount < batchSize; ++txnEventCount) {
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

        //3) Write the data to Hive
        try {
          writer.write(event);
        } catch (Exception ex) {
          LOG.warn(ex.getMessage(), ex);
        }

      } // for

      //4) Update counters
      if (txnEventCount == 0) {
        sinkCounter.incrementBatchEmptyCount();
      } else if (txnEventCount == batchSize) {
        sinkCounter.incrementBatchCompleteCount();
      } else {
        sinkCounter.incrementBatchUnderflowCount();
      }

      // 5) Flush all Writers and Commit Channel Txn
      for (HiveWriter writer : activeWriters.values()) {
        writer.flush(true);
      }

      transaction.commit();

      if (txnEventCount < 1) {
        return Status.BACKOFF;
      } else {
        sinkCounter.addToEventDrainSuccessCount(txnEventCount);
        return Status.READY;
      }
    } catch (StreamingException err) {
      transaction.rollback();
      LOG.warn("Hive streaming error", err);
      retireIdleWriters();
      return Status.BACKOFF;
    } catch (Throwable th) {
      transaction.rollback();
      LOG.error("process failed", th);
      if (th instanceof Error) {
        throw (Error) th;
      } else {
        throw new EventDeliveryException(th);
      }
    } finally {
      transaction.close();
    }
  }

  private void enableHeartBeatOnAllWriters() {
    for (HiveWriter writer : allWriters.values()) {
      writer.setHearbeatNeeded();
    }
  }

  private HiveWriter getOrCreateWriter(HashMap<HiveEndPoint, HiveWriter> activeWriters,
                                       HiveEndPoint endPoint)
          throws IOException, InterruptedException, ClassNotFoundException, StreamingException {
    try {
      HiveWriter writer = allWriters.get( endPoint );
      if( writer == null ) {
        LOG.info("Creating Writer to Hive end point : " + endPoint);
        writer = new HiveWriter(endPoint, txnsPerBatchAsk, autoCreatePartitions,
                callTimeout, callTimeoutPool, proxyUser, serializer, sinkCounter);

        sinkCounter.incrementConnectionCreatedCount();
        if(allWriters.size() > maxOpenConnections){
          int retired = retireIdleWriters();
          if(retired==0) {
            retireEldestWriter();
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
    } catch (ClassNotFoundException e) {
      LOG.error("Failed to create HiveWriter for endpoint: " + endPoint, e);
      sinkCounter.incrementConnectionFailedCount();
      throw e;
    } catch (StreamingException e) {
      LOG.error("Failed to create HiveWriter for endpoint: " + endPoint, e);
      sinkCounter.incrementConnectionFailedCount();
      throw e;
    }

  }

  private HiveEndPoint makeEndPoint(String metaStoreUri, String database, String table,
                                    List<String> partVals, Map<String, String> headers,
                                    TimeZone timeZone, boolean needRounding,
                                    int roundUnit, Integer roundValue,
                                    boolean useLocalTime) throws ConnectionError {
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
  private void retireEldestWriter() {
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
      LOG.info("Closing least used Writer to Hive end point : " + eldest);
      allWriters.remove(eldest).close();
    } catch (IOException e) {
      LOG.warn("Failed to close writer for end point: " + eldest, e);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted when attempting to close writer for end point: " + eldest, e);
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Locate all writers past idle timeout and retire them
   * @return number of writers retired
   */
  private int retireIdleWriters() {
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
      try {
        sinkCounter.incrementConnectionClosedCount();
        LOG.info("Closing idle Writer to Hive end point : {}", ep);
        allWriters.remove(ep).close();
      } catch (IOException e) {
        LOG.warn("Failed to close writer for end point: {}. Error: "+ ep, e);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted when attempting to close writer for end point: " + ep, e);
        Thread.currentThread().interrupt();
      }
    }
    return count;
  }

  @Override
  public void stop() {
    // do not constrain close() calls with a timeout
    for (Entry<HiveEndPoint, HiveWriter> entry : allWriters.entrySet()) {
      try {
        HiveWriter w = entry.getValue();
        LOG.info("Flushing writer to {}", w);
        w.flush(false);
        LOG.info("Closing writer to {}", w);
        w.close();
      } catch (Exception ex) {
        LOG.warn("Error while closing writer to " + entry.getKey() +
                ". Exception follows.", ex);
        if (ex instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
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
        LOG.warn("shutdown interrupted on " + execService, ex);
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
    LOG.info("Hive Sink {} started", getName() );
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

}
