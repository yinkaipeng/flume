/*
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

package org.apache.flume.channel;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import java.util.UUID;

import org.apache.flume.*;
import org.apache.flume.channel.file.FileChannel;
import org.apache.flume.channel.file.FileChannelConfiguration;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;


public class TestSpillableMemoryChannel {

  private SpillableMemoryChannel channel;
  private FileChannel fileChannel;
  private String fileChannelName;

  private MemoryChannel memoryChannel;
  private String memoryChannelName;

  private Map<String, String> fileChannelOverrides = new HashMap<String, String>();

  private Map<String,AbstractChannel> channelMap;


  @Rule
  public TemporaryFolder fileChannelDir = new TemporaryFolder();


  public static Context createFileChannelContext(String checkpointDir, String dataDir, Map<String, String> overrides) {
    Context context = new Context();
    context.put(FileChannelConfiguration.CHECKPOINT_DIR, checkpointDir);
    context.put(FileChannelConfiguration.DATA_DIRS, dataDir);
    context.put(FileChannelConfiguration.KEEP_ALIVE, String.valueOf(1));
    context.put(FileChannelConfiguration.CAPACITY, String.valueOf(500020));
    // Set checkpoint for 5 seconds otherwise test will run out of memory
    context.put(FileChannelConfiguration.CHECKPOINT_INTERVAL, "5000");
    context.put(FileChannelConfiguration.KEEP_ALIVE, "0");

    if(overrides!=null)
      context.putAll(overrides);
    return context;
  }

  public void configureFileChannel(Map<String, String> overrides) {
    File checkPointDir = fileChannelDir.newFolder("checkpoint");
    File dataDir = fileChannelDir.newFolder("data");
    Context context = createFileChannelContext(checkPointDir.getAbsolutePath(), dataDir.getAbsolutePath(), overrides );
    Configurables.configure(fileChannel, context);
  }

  private void configureChannel(Map<String, String> params) {
    Context context = new Context();
    context.putAll(params);
    Configurables.configure(channel, context);
    channel.postConfigure(channelMap);
  }

  private void startChannel(Map<String, String> params) {
    configureChannel(params);
    channel.start();
  }

  private void restartChannel_withFC(Map<String, String> params, Map<String, String> fcParams) {
    channel.stop();
    configureFileChannel(fcParams);
    startChannel(params);
  }


  static class NullFound extends RuntimeException {
    public int expectedValue;
    public NullFound(int expected) {
      super("Expected " + expected + ",  but null found");
      expectedValue=expected;
    }
  }

  static class TooManyNulls extends RuntimeException {
    private int nullsFound;
    public TooManyNulls(int count) {
      super("Total nulls found in thread (" + Thread.currentThread().getName() + ") : " + count);
      nullsFound=count;
    }
  }

  @Before
  public void setUp() {
    channel = new SpillableMemoryChannel();
    channel.setName("spillChannel-" + UUID.randomUUID() );

    fileChannel =  new FileChannel();
    fileChannelName = "fileChannel-" + UUID.randomUUID();
    fileChannel.setName(fileChannelName);
    configureFileChannel(null);

    memoryChannel = new MemoryChannel();
    memoryChannelName = "memChannel-" + UUID.randomUUID();
    memoryChannel.setName(memoryChannelName);
    Context context = new Context();
    context.put("capacity","500020");
    context.put("keep-alive", "0");
    Configurables.configure(memoryChannel, context);

    channelMap = new HashMap<String, AbstractChannel>(2);
    channelMap.put(fileChannel.getName(), fileChannel);
    channelMap.put(memoryChannel.getName(), memoryChannel);
  }

  @After
  public void tearDown() {
    channel.stop();
  }

  private static void putN(int first, int count, AbstractChannel channel) {
    for(int i=0; i<count; ++i) {
      channel.put(EventBuilder.withBody(String.valueOf(first++).getBytes()));
    }
  }

  private static void takeNull(AbstractChannel channel) {
      Event e = channel.take();
//      Assert.assertNull(e);
  }

  private static void takeN(int first, int count, AbstractChannel channel) {
    int last = first + count;
    for(int i=first; i<last; ++i) {
      Event e = channel.take();
      if(e==null) {
        throw new NullFound(i);
      }
      Event expected = EventBuilder.withBody( String.valueOf(i).getBytes() );
      Assert.assertArrayEquals(e.getBody(), expected.getBody());
    }
  }

  private static int takeN_NoCheck(int count, AbstractChannel channel) throws InterruptedException {
    final int MAXNULLS = 1000000;
    int nullsFound = 0;
    for(int i=0; i<count;) {
      Event e = channel.take();
      if(e!=null) {
        ++i;
      } else {
        ++nullsFound;
//        if(nullsFound >= MAXNULLS)
//           throw new TooManyNulls(nullsFound);
//        Thread.sleep(0);
      }
    }
    return nullsFound;
  }

  private static void transactionalPutN(int first, int count, AbstractChannel channel) {
    Transaction tx = channel.getTransaction();
    tx.begin();
    try {
      putN(first, count, channel);
      tx.commit();
    } catch (RuntimeException e) {
      System.out.println("Queue size is " +  ( (SpillableMemoryChannel) channel).queueSize());
      tx.rollback();
      throw e;
    } catch (Throwable e) {
      System.out.println("Queue size is " +  ( (SpillableMemoryChannel) channel).queueSize());
      e.printStackTrace();
    } finally {
      tx.close();
    }
  }

  private static void transactionalTakeN(int first, int count, AbstractChannel channel) {
    Transaction tx = channel.getTransaction();
    tx.begin();
    try {
      takeN(first, count, channel);
      tx.commit();
    } catch (NullFound e) {
      tx.commit();
      throw e;
    } catch (AssertionError e) {
      tx.rollback();
      throw e;
    } catch (RuntimeException e) {
      tx.rollback();
      throw e;
    } catch (Throwable e) {
      System.out.println("Queue size is " + ((SpillableMemoryChannel) channel).queueSize());
      e.printStackTrace();
    } finally {
      tx.close();
    }
  }

  private static int transactionalTakeN_NoCheck(int count, AbstractChannel channel) throws InterruptedException {
    Transaction tx = channel.getTransaction();
    tx.begin();
    try {
      int nullCount = takeN_NoCheck(count, channel);
      tx.commit();
      return  nullCount;
    } catch (RuntimeException e) {
      tx.rollback();
      throw e;
    } catch (Throwable e) {
      System.out.println("Queue size is " +  ( (SpillableMemoryChannel) channel).queueSize());
      e.printStackTrace();
      return -1;
    } finally {
      tx.close();
    }
  }

  private static void transactionalTakeNull(int count, AbstractChannel channel) {
    Transaction tx = channel.getTransaction();
    tx.begin();
    try {
      for(int i = 0; i<count; ++i)
        takeNull(channel);
      tx.commit();
    } catch (AssertionError e) {
      tx.rollback();
      throw e;
    } catch (RuntimeException e) {
      tx.rollback();
      throw e;
    } finally {
      tx.close();
    }
  }

  private Thread makePutThread(String threadName, final int first, final int count, final int batchSize, final AbstractChannel channel) {
    return
      new Thread(threadName) {
        public void run() {
          int maxdepth = 0;
          StopWatch watch = new StopWatch();
          for(int i = first; i<first+count; i=i+batchSize) {
            transactionalPutN(i, batchSize, channel);
//            int depth =  ((FileChannel) fileChannel).getDepth();
//            if(maxdepth < depth )
//              maxdepth = depth;
          }
          watch.elapsed();
//          System.out.println(Thread.currentThread().getName() + " is done.");
          System.out.println("FC Max queue size " + maxdepth);
        }
      };
  }

  private static Thread makeTakeThread(String threadName, final int first, final int count, final int batchSize, final AbstractChannel channel) {
    return
      new Thread(threadName) {
        public void run() {
          StopWatch watch = new StopWatch();
          for(int i = first; i<first+count; ) {
            try {
              transactionalTakeN(i, batchSize, channel);
              i = i + batchSize;
            } catch (NullFound e) {
              i = e.expectedValue;
            }
          }
          watch.elapsed();
//          System.out.println(Thread.currentThread().getName() + " is done.");
        }
      };
  }

  private static Thread makeTakeThread_noCheck(String threadName, final int count, final int batchSize, final AbstractChannel channel) {
    return
      new Thread(threadName) {
        public void run() {
          StopWatch watch = new StopWatch();
          int i = 0, nulls=0 ;
          while(i<count) {
            try {
              nulls += transactionalTakeN_NoCheck(batchSize, channel);
              i+=batchSize;
            } catch (NullFound e) {
              i = e.expectedValue;
            } catch (InterruptedException e) {
              e.printStackTrace();
              return;
            }
          }
          watch.elapsed(" items = " + i + ", nulls = " + nulls);
        }
      };
  }

  @Test
  public void testPutTake() {
    Map<String, String> params = new HashMap<String, String>();
    params.put("overflowChannel", fileChannelName);
    params.put("maxTransactionBatchSize", "2");
    params.put("memoryCapacity", "5");
    params.put("totalCapacity", "10");
    startChannel(params);

    Transaction tx = channel.getTransaction();
    tx.begin();
    putN(0,2,channel);
    tx.commit();
    tx.close();

    tx = channel.getTransaction();
    tx.begin();
    takeN(0,2,channel);
    tx.commit();
    tx.close();
  }


  @Test
  public void testCapacityDisableOverflow()  {
    Context context = new Context();
    Map<String, String> params = new HashMap<String, String>();
    params.put("overflowChannel", fileChannelName);
    params.put("maxTransactionBatchSize", "2");
    params.put("memoryCapacity", "2");
    params.put("totalCapacity", "2");   // overflow is disabled effectively as this is == memoryCapacity
    params.put("keep-alive", "0" );
    startChannel(params);

    transactionalPutN(0,2,channel);

    boolean threw = false;
    try {
      transactionalPutN(2,1,channel);
    } catch (ChannelException e) {
      threw = true;
    }
    Assert.assertTrue(threw);

    transactionalTakeN(0,2, channel);

    Transaction tx = channel.getTransaction();
    tx.begin();
    Assert.assertNull(channel.take());
    tx.commit();
    tx.close();
  }

  @Test
  public void testCapacityWithOverflow()  {
    Map<String, String> params = new HashMap<String, String>();
    params.put("overflowChannel", fileChannelName);
    params.put("maxTransactionBatchSize", "2");
    params.put("memoryCapacity", "2");
    params.put("totalCapacity", "4");
    params.put("keep-alive", "0");
    startChannel(params);

    transactionalPutN(1, 2, channel);
    transactionalPutN(3, 2, channel);

    boolean threw = false;
    try {
      transactionalPutN(5,1,channel);
    } catch (ChannelException e) {
      threw = true;
    }
    Assert.assertTrue(threw);

    transactionalTakeN(1,2, channel);
    transactionalTakeN(3,2, channel);

  }


  @Test
  public void testOverflow() {

    Map<String, String> params = new HashMap<String, String>();
    params.put("overflowChannel", fileChannelName);
    params.put("maxTransactionBatchSize", "5");
    params.put("memoryCapacity", "10");
    params.put("totalCapacity", "20");
    params.put("keep-alive", "1" );

    startChannel(params);

    transactionalPutN( 1,5,channel);
    transactionalPutN( 5,5,channel);
    transactionalPutN(10,5,channel); // these should go to overflow

    transactionalTakeN(10,5, fileChannel);
  }

  @Test
  public void testDrainOrder() {
    Context context = new Context();
    Map<String, String> params = new HashMap<String, String>();
    params.put("overflowChannel", fileChannelName);
    params.put("maxTransactionBatchSize", "5");
    params.put("memoryCapacity", "10");
    params.put("totalCapacity", "20");
    params.put("keep-alive", "1" );

    context.putAll(params);
    Configurables.configure(channel, context);
    channel.postConfigure(channelMap);
    channel.start();

    transactionalPutN( 1,5,channel);
    transactionalPutN( 6,5,channel);
    transactionalPutN(11,5,channel); // into overflow
    transactionalPutN(16,5,channel); // into overflow

    transactionalTakeN(1, 1, channel);
    transactionalTakeN(2, 5,channel);
    transactionalTakeN(7, 4,channel);

    transactionalPutN( 20,2,channel);
    transactionalPutN( 22,3,channel);

    transactionalTakeN( 11,3,channel); // from overflow
    transactionalTakeN( 14,5,channel); // from overflow
    transactionalTakeN( 19,2,channel); // from overflow
  }

  @Test
  public void testDrainingOnChannelBoundary() {

    Map<String, String> params = new HashMap<String, String>();
    params.put("overflowChannel", fileChannelName);
    params.put("maxTransactionBatchSize", "10");
    params.put("memoryCapacity", "10");
    params.put("totalCapacity", "20");
    params.put("keep-alive", "1" );
    startChannel(params);

    transactionalPutN(1, 5, channel);
    transactionalPutN(6, 5, channel);  // into overflow
    transactionalPutN(11, 5, channel);  // into overflow
    transactionalPutN(16, 5, channel);  // into overflow

    transactionalTakeN(1, 3, channel);

    Transaction tx = channel.getTransaction();
    tx.begin();
    takeN(4, 2, channel);
    takeNull(channel);  // expect null since next event is in overflow
    tx.commit();
    tx.close();

    transactionalTakeN(6, 5, channel);  // from overflow

    transactionalTakeN(11, 5, channel); // from overflow
    transactionalTakeN(16, 2,channel); // from overflow

    transactionalPutN(21, 5, channel);

    tx = channel.getTransaction();
    tx.begin();
    takeN(18,3, channel);              // from overflow
    takeNull(channel);  // expect null since next event is in primary
    tx.commit();
    tx.close();

    transactionalTakeN(21, 5, channel);
  }

  @Test
  public void testRollBack() {
    Map<String, String> params = new HashMap<String, String>();
    params.put("overflowChannel", memoryChannelName);
    params.put("maxTransactionBatchSize", "10");
    params.put("memoryCapacity", "100");
    params.put("totalCapacity", "1000");
    params.put("keep-alive", "0");
    startChannel(params);


    //1 Rollback for Puts
    transactionalPutN(1,5, channel);
    Transaction tx = channel.getTransaction();
    tx.begin();
    putN(6, 5, channel);
    tx.rollback();
    tx.close();

    transactionalTakeN(1, 5, channel);
    transactionalTakeNull(2, channel);

    //2.  verify things back to normal after put rollback
    transactionalPutN(11, 5, channel);
    transactionalTakeN(11,5,channel);


    //3 Rollback for Takes
    transactionalPutN(16, 5, channel);
    tx = channel.getTransaction();
    tx.begin();
    takeN(16, 5, channel);
    takeNull(channel);
    tx.rollback();
    tx.close();

    transactionalTakeN(16, 5, channel);

    //4.  verify things back to normal after take rollback
    transactionalPutN(21,5, channel);
    transactionalTakeN(21,5,channel);
  }


  @Test
  public void testReconfigure()  {
    //1) bring up with small capacity
    Map<String, String> params = new HashMap<String, String>();
    params.put("overflowChannel", fileChannelName);
    params.put("maxTransactionBatchSize", "10");
    params.put("memoryCapacity", "10");
    params.put("totalCapacity", "10");
    params.put("keep-alive", "0");
    startChannel(params);

    Assert.assertTrue("keep-alive setting did not reconfigure correctly", channel.getKeepAlive()==0);
    Assert.assertTrue("maxTransactionBatchSize did not reconfigure correctly", channel.getTransBatchSize() == 10 );
    Assert.assertTrue("memoryCapacity did not reconfigure correctly", channel.getMemoryCapacity()==10);
    Assert.assertTrue("totalCapacity did not reconfigure correctly", channel.getTotalCapacity()==10);

    transactionalPutN(1, 10, channel);
    boolean threw = false;
    try {
      transactionalPutN(11, 10, channel);  // should throw an error
    } catch (ChannelException e) {
      threw = true;
    }
    Assert.assertTrue("Expected the channel to fill up and throw an exception, but it did not throw", threw);


    //2) Resize and verify
    params = new HashMap<String, String>();
    params.put("overflowChannel", fileChannelName);
    params.put("maxTransactionBatchSize", "10");
    params.put("memoryCapacity", "20");
    params.put("totalCapacity", "20");
    restartChannel_withFC(params, null);

    Assert.assertTrue("keep-alive setting did not reconfigure correctly", channel.getKeepAlive()==SpillableMemoryChannel.defaultKeepAlive);
    Assert.assertTrue("maxTransactionBatchSize did not reconfigure correctly", channel.getTransBatchSize() == 10 );
    Assert.assertTrue("memoryCapacity did not reconfigure correctly", channel.getMemoryCapacity()==20);
    Assert.assertTrue("totalCapacity did not reconfigure correctly", channel.getTotalCapacity()==20);

    transactionalTakeN(1, 10, channel); // pull out the values inserted prior to reconfiguration

    transactionalPutN(11, 10, channel);
    transactionalPutN(21, 10, channel);

    threw = false;
    try {
      transactionalPutN(31, 10, channel);  // should throw an error
    } catch (ChannelException e) {
      threw = true;
    }
    Assert.assertTrue("Expected the channel to fill up and throw an exception, but it did not throw", threw);

    transactionalTakeN(11, 10, channel);
    transactionalTakeN(21, 10, channel);


    // 3) Reconfigure with empty config and verify settings revert to deafult
    params = new HashMap<String, String>();
    params.put("overflowChannel", fileChannelName);
    restartChannel_withFC(params, null);

    Assert.assertTrue("keep-alive setting did not reconfigure correctly", channel.getKeepAlive()==SpillableMemoryChannel.defaultKeepAlive);
    Assert.assertTrue("maxTransactionBatchSize did not reconfigure correctly", channel.getTransBatchSize() == SpillableMemoryChannel.defaultMaxTransactionBatchSize );
    Assert.assertTrue("memoryCapacity did not reconfigure correctly", channel.getMemoryCapacity()==SpillableMemoryChannel.defaultMemoryCapacity);
    Assert.assertTrue("totalCapacity did not reconfigure correctly", channel.getTotalCapacity()==SpillableMemoryChannel.totalCapacityUnlimited);


    // 4) Reconfiguring of  overflow
    params = new HashMap<String, String>();
    params.put("capacity", "5");  // sized to fill up before totalCapacity of SpillableChannel is reached
    configureFileChannel(params);

    params = new HashMap<String, String>();
    params.put("overflowChannel", fileChannelName);
    params.put("maxTransactionBatchSize", "10");
    params.put("memoryCapacity", "10");
    params.put("totalCapacity", "20");
    params.put("keep-alive", "1");
    configureChannel(params);


    Assert.assertTrue("keep-alive setting not configured correctly", channel.getKeepAlive()==1);
    Assert.assertTrue("maxTransactionBatchSize not configured correctly", channel.getTransBatchSize() == 10 );
    Assert.assertTrue("memoryCapacity not configured correctly", channel.getMemoryCapacity()==10);
    Assert.assertTrue("totalCapacity not configured correctly", channel.getTotalCapacity()==20);
//    Assert.assertTrue("capacity for file Channel not configured correctly", fileChannel.getCapacity()==20);


    transactionalPutN( 1,10, channel);
    transactionalPutN(11,5, channel);
    threw=false;
    try {
      transactionalPutN(15,6, channel); // should error out as both primary & overflow are full
    } catch (ChannelException e) {
      threw = true;
    }
    Assert.assertTrue("Expected the last insertion to fail, but it didn't.", threw);

    // reconfig the overflow
    params = new HashMap<String, String>();
    params.put("capacity", "10");  // bump up the capacit
    configureFileChannel(params);

    params = new HashMap<String, String>();
    params.put("overflowChannel", fileChannelName);
    params.put("maxTransactionBatchSize", "10");
    params.put("memoryCapacity", "10");
    params.put("totalCapacity", "20");
    params.put("keep-alive", "1");
    configureChannel(params);

    transactionalPutN(15,5, channel);  // show succeed now as we have made room in the overflow
    transactionalTakeN(1,10, channel);
    transactionalTakeN(11,5, channel);
    transactionalTakeN(15, 5, channel);
  }

  @Test
  public void testPerf() {

    Map<String, String> params = new HashMap<String, String>();
    params.put("overflowChannel", memoryChannelName);
    params.put("maxTransactionBatchSize", "100");
    params.put("memoryCapacity", "100020");
    params.put("totalCapacity", "100020");
    startChannel(params);

    long startTime0 = System.currentTimeMillis();
    long totalPutTime=0, totalTakeTime=0;

    for(int x=0; x<10; ++x) {
      long startTime = System.currentTimeMillis();
      for(int i =0; i<100000; i=i+100)
        transactionalPutN(i,100,channel);

      long elapsed = System.currentTimeMillis() - startTime;
      totalPutTime+= elapsed;
      System.out.println("Total put time: " + elapsed + " ms" );

      startTime = System.currentTimeMillis();
      for(int i =0; i<100000; i=i+100)
        transactionalTakeN(i,100,channel);
      elapsed = System.currentTimeMillis() - startTime;
      totalTakeTime+= elapsed;
      System.out.println("Total take time: " + elapsed + "ms" );
    }

    System.out.println();
    System.out.println("TOTAL put  time: " + totalPutTime + " ms" );
    System.out.println("TOTAL take time: " + totalTakeTime + " ms" );
    System.out.println("TOTAL exec time: " + ( System.currentTimeMillis() - startTime0) + " ms" );

  }

  @Test
  public void testParallelSingleSourceAndSink() throws InterruptedException {
    Map<String, String> params = new HashMap<String, String>();
    params.put("overflowChannel", memoryChannelName);
    params.put("maxTransactionBatchSize", "100");
    params.put("memoryCapacity", "1000020");
    params.put("totalCapacity",   "1000020");
    params.put("keep-alive", "3");
    startChannel(params);

    Thread sourceThd = makePutThread("putter", 1, 500000, 100, channel);
    Thread sinkThd =  makeTakeThread("taker",  1, 500000, 100, channel);

    StopWatch watch = new StopWatch();

    sinkThd.start();
    sourceThd.start();

    sourceThd.join();
    sinkThd.join();

    watch.elapsed();
    System.out.println("Max Queue size " + channel.getMaxQueueSize() );
  }


  public ArrayList<Thread> createSourceThreads(int count, int totalEvents, int batchSize) {
    ArrayList<Thread> sourceThds = new ArrayList<Thread>();

    for(int i=0; i<count; ++i) {
      sourceThds.add(  makePutThread("PUTTER" + i, 1, totalEvents/count, batchSize, channel) );
    }
    return sourceThds;
  }

  public ArrayList<Thread> createSinkThreads(int count, int totalEvents, int batchSize) {
    ArrayList<Thread> sinkThreads = new ArrayList<Thread>(count);

    for(int i=0; i<count; ++i) {
      sinkThreads.add( makeTakeThread_noCheck("taker"+i, totalEvents/count, batchSize, channel) );
    }
    return sinkThreads;
  }

  public void startThreads(ArrayList<Thread> threads) {
    for(Thread thread : threads ) {
      thread.start();
    }
  }

  public void joinThreads(ArrayList<Thread> threads) throws InterruptedException {
    for(Thread thread : threads ) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        System.out.println("Interrupted while waiting on " + thread.getName() );
        throw e;
      }
    }
  }

  @Test
  public void testParallelMultipleSourcesAndSinks() throws InterruptedException {
    int sourceCount = 1;
    int sinkCount = 1;
    int eventCount = 1000000;
    int batchSize = 100;

    Map<String, String> params = new HashMap<String, String>();
    params.put("overflowChannel", fileChannelName);
    params.put("maxTransactionBatchSize", "" + batchSize);
    params.put("memoryCapacity", "0");
    params.put("totalCapacity",  "500020");
    params.put("keep-alive", "3");
    startChannel(params);

    ArrayList<Thread> sinks = createSinkThreads(sinkCount, eventCount, batchSize);

    ArrayList<Thread> sources =  createSourceThreads(sourceCount, eventCount, batchSize);


    StopWatch watch = new StopWatch();
    startThreads(sinks);
    startThreads(sources);

    joinThreads(sources);
    joinThreads(sinks);

    watch.elapsed();

    System.out.println("Total puts " + channel.drainOrder.totalPuts);

    System.out.println("Max Queue size " + channel.getMaxQueueSize() );
    System.out.println(channel.queue.size());

    System.out.println("done");
  }


  static class StopWatch {
    long startTime;

    public StopWatch() {
      startTime = System.currentTimeMillis();
    }

    public void elapsed() {
      elapsed(null);
    }

    public void elapsed(String suffix) {
      long elapsed = System.currentTimeMillis() - startTime;
      if(suffix==null)
        suffix = "";
      else
        suffix = "{ " + suffix + " }";

      if (elapsed < 10000)
        System.out.println(Thread.currentThread().getName() +  " : [ " + elapsed + " ms ].        " + suffix);
      else
        System.out.println(Thread.currentThread().getName() +  " : [ " + elapsed / 1000 + " sec ].       " + suffix);
    }
  }


}


// TODO:
// Parallel runs multiple sources/sinks is indicating some issue. Sink is slower than source
//