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
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.channel.file.FileChannelConfiguration;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;


public class TestSpillableMemoryChannel {

  private SpillableMemoryChannel channel;

  @Rule
  public TemporaryFolder fileChannelDir = new TemporaryFolder();

  private void configureChannel(Map<String, String> overrides) {
    Context context = new Context();
    File checkPointDir = fileChannelDir.newFolder("checkpoint");
    File dataDir = fileChannelDir.newFolder("data");
    context.put(FileChannelConfiguration.CHECKPOINT_DIR, checkPointDir.getAbsolutePath());
    context.put(FileChannelConfiguration.DATA_DIRS, dataDir.getAbsolutePath());
    // Set checkpoint for 5 seconds otherwise test will run out of memory
    context.put(FileChannelConfiguration.CHECKPOINT_INTERVAL, "5000");

    if(overrides!=null)
      context.putAll(overrides);

    Configurables.configure(channel, context);
  }

  private void reconfigureChannel(Map<String, String> overrides) {
    channel.stop();
    configureChannel(overrides);
    channel.start();
  }

  private void startChannel(Map<String, String> params) {
    configureChannel(params);
    channel.start();
  }

  // performs a hard restart of the channel... creates a new channel object
  private void restartChannel(Map<String, String> params) {
    channel.stop();
    setUp();
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
    configureChannel(null);
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
        if(nullsFound >= MAXNULLS)
           throw new TooManyNulls(nullsFound);
        Thread.sleep(0);
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
//      System.out.println("Queue size is " +  ( (SpillableMemoryChannel) channel).queueSize());
      tx.rollback();
      throw e;
    } catch (Throwable e) {
//      System.out.println("Queue size is " +  ( (SpillableMemoryChannel) channel).queueSize());
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
//      System.out.println("Queue size is " + ((SpillableMemoryChannel) channel).queueSize());
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
//      System.out.println("Queue size is " +  ( (SpillableMemoryChannel) channel).queueSize());
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
          }
          watch.elapsed();
//          System.out.println("FC Max queue size " + maxdepth);
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
    params.put("memoryCapacity", "5");
    params.put("overflowCapacity", "5");
    params.put(FileChannelConfiguration.TRANSACTION_CAPACITY, "5");
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
    Map<String, String> params = new HashMap<String, String>();
    params.put("memoryCapacity", "2");
    params.put("overflowCapacity", "0");   // overflow is disabled effectively
    params.put("overflowTimeout", "0" );
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
    params.put("memoryCapacity", "2");
    params.put("overflowCapacity", "4");
    params.put(FileChannelConfiguration.TRANSACTION_CAPACITY, "3");
    params.put("overflowTimeout", "0");
    startChannel(params);

    transactionalPutN(1, 2, channel);
    transactionalPutN(3, 2, channel);
    transactionalPutN(5, 2, channel);

    boolean threw = false;
    try {
      transactionalPutN(7,2,channel);   // cannot fit in channel
    } catch (Exception e) {
      threw = true;
    }
    Assert.assertTrue(threw);

    transactionalTakeN(1,2, channel);
    transactionalTakeN(3,2, channel);
    transactionalTakeN(5,2, channel);
  }

  @Test
  public void testRestart()  {
    Map<String, String> params = new HashMap<String, String>();
    params.put("memoryCapacity", "2");
    params.put("overflowCapacity", "10");
    params.put(FileChannelConfiguration.TRANSACTION_CAPACITY, "4");
    params.put("overflowTimeout", "0");
    startChannel(params);

    transactionalPutN(1, 2, channel);
    transactionalPutN(3, 2, channel);  // goes in overflow

    restartChannel(params);

    transactionalTakeN(3,2, channel);  // from overflow, as in memory stuff should be lost

  }

  @Test
  public void testOverflow() {

    Map<String, String> params = new HashMap<String, String>();
    params.put("memoryCapacity", "10");
    params.put("overflowCapacity", "20");
    params.put(FileChannelConfiguration.TRANSACTION_CAPACITY, "10");
    params.put("overflowTimeout", "1" );

    startChannel(params);

    transactionalPutN( 1,5,channel);
    transactionalPutN( 6,5,channel);
    transactionalPutN(11,5,channel); // these should go to overflow

    transactionalTakeN(1,10, channel);
    transactionalTakeN(11,5, channel);
  }

  @Test
  public void testDrainOrder() {
    Map<String, String> params = new HashMap<String, String>();
    params.put("memoryCapacity", "10");
    params.put("overflowCapacity", "10");
    params.put(FileChannelConfiguration.TRANSACTION_CAPACITY, "5");
    params.put("overflowTimeout", "1" );

    startChannel(params);

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
  public void testByteCapacity()  {
    Map<String, String> params = new HashMap<String, String>();
    params.put("memoryCapacity", "1000");
    // configure to hold 8 events of 10 bytes each (plus 20% event header space)
    params.put("byteCapacity", "100");
    params.put("avgEventSize", "10");
    params.put("overflowCapacity", "20");
    params.put(FileChannelConfiguration.TRANSACTION_CAPACITY, "10");
    params.put("overflowTimeout", "1" );
    startChannel(params);

    transactionalPutN(1, 8, channel);   // this wil max the byteCapacity
    transactionalPutN(9, 10, channel);
    transactionalPutN(19,10, channel);  // this will fill up the overflow

    boolean threw = false;
    try {
      transactionalPutN(11, 1, channel);  // into overflow
    } catch (Exception e) {
      threw = true;
    }
    Assert.assertTrue("byteCapacity did not work as expected",threw);

  }

  @Test
  public void testDrainingOnChannelBoundary() {

    Map<String, String> params = new HashMap<String, String>();
    params.put("memoryCapacity", "5");
    params.put("overflowCapacity", "15");
    params.put(FileChannelConfiguration.TRANSACTION_CAPACITY, "10");
    params.put("overflowTimeout", "1" );
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
    params.put("memoryCapacity", "100");
    params.put("overflowCapacity", "900");
    params.put(FileChannelConfiguration.TRANSACTION_CAPACITY, "900");
    params.put("overflowTimeout", "0");
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

    try {
      transactionalTakeN_NoCheck(5, channel);
    } catch (InterruptedException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

    //4.  verify things back to normal after take rollback
    transactionalPutN(21,5, channel);
    transactionalTakeN(21,5,channel);
  }


  @Test
  public void testReconfigure()  {
    //1) bring up with small capacity
    Map<String, String> params = new HashMap<String, String>();
    params.put("memoryCapacity", "10");
    params.put("overflowCapacity", "0");
    params.put("overflowTimeout", "0");
    startChannel(params);

    Assert.assertTrue("overflowTimeout setting did not reconfigure correctly", channel.getOverflowTimeout()==0);
    Assert.assertTrue("memoryCapacity did not reconfigure correctly", channel.getMemoryCapacity()==10);
    Assert.assertTrue("overflowCapacity did not reconfigure correctly", channel.isOverflowDisabled()==true );

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
    params.put("memoryCapacity", "20");
    params.put("overflowCapacity", "0");
    reconfigureChannel(params);

    Assert.assertTrue("overflowTimeout setting did not reconfigure correctly", channel.getOverflowTimeout()==SpillableMemoryChannel.defaultOverflowTimeout);
    Assert.assertTrue("memoryCapacity did not reconfigure correctly", channel.getMemoryCapacity()==20);
    Assert.assertTrue("overflowCapacity did not reconfigure correctly", channel.isOverflowDisabled()==true );

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


    // 3) Reconfigure with empty config and verify settings revert to default
    params = new HashMap<String, String>();
    reconfigureChannel(params);

    Assert.assertTrue("overflowTimeout setting did not reconfigure correctly", channel.getOverflowTimeout()==SpillableMemoryChannel.defaultOverflowTimeout);
    Assert.assertTrue("memoryCapacity did not reconfigure correctly", channel.getMemoryCapacity()==SpillableMemoryChannel.defaultMemoryCapacity);
    Assert.assertTrue("overflowCapacity did not reconfigure correctly", channel.getOverflowCapacity()== SpillableMemoryChannel.defaultOverflowCapacity);
    Assert.assertTrue("overflowCapacity did not reconfigure correctly", channel.isOverflowDisabled()==false);


    // 4) Reconfiguring of  overflow
    params = new HashMap<String, String>();
    params.put("memoryCapacity", "10");
    params.put("overflowCapacity", "10");
    params.put("transactionCapacity", "5");
    params.put("overflowTimeout", "1");
    reconfigureChannel(params);

    transactionalPutN( 1,5, channel);
    transactionalPutN( 6,5, channel);
    transactionalPutN(11,5, channel);
    transactionalPutN(16,5, channel);
    threw=false;
    try {
      transactionalPutN(21,5, channel); // should error out as both primary & overflow are full
    } catch (ChannelException e) {
      threw = true;
    }
    Assert.assertTrue("Expected the last insertion to fail, but it didn't.", threw);

    // reconfig the overflow
    params = new HashMap<String, String>();
    params.put("memoryCapacity", "10");
    params.put("overflowCapacity", "20");
    params.put("transactionCapacity", "10");
    params.put("overflowTimeout", "1");
    reconfigureChannel(params);

    transactionalPutN(21,5, channel);  // should succeed now as we have made room in the overflow

    transactionalTakeN(1,10, channel);
    transactionalTakeN(11,5, channel);
    transactionalTakeN(16, 5, channel);
    transactionalTakeN(21, 5, channel);
  }

  @Test
  public void testParallelSingleSourceAndSink() throws InterruptedException {
    Map<String, String> params = new HashMap<String, String>();
    params.put("memoryCapacity", "1000020");
    params.put("overflowCapacity",   "0");
    params.put("overflowTimeout", "3");
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
    params.put("memoryCapacity", "0");
    params.put("overflowCapacity",  "500020");
    params.put("overflowTimeout", "3");
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
    System.out.println(channel.memQueue.size());

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
