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

import java.util.ArrayDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.GuardedBy;

import org.apache.flume.*;
import org.apache.flume.annotations.Recyclable;

import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.channel.file.FileChannel;
import org.apache.flume.instrumentation.ChannelCounter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * <p>
 * SpillableMemoryChannel will use main memory for buffering events until it has reached capacity.
 * Thereafter another channel (such as file channel) will be used to buffer the events.
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@Recyclable
public class SpillableMemoryChannel extends FileChannel {
  private static Logger LOGGER = LoggerFactory.getLogger(SpillableMemoryChannel.class);
  public static final int defaultMemoryCapacity = 10000;
  public static final int defaultOverflowCapacity = 100000000;

  public static final int defaultOverflowTimeout = 3;
  public static final int defaultOverflowDeactivationThreshold = 5; // percent

  private Object queueLock = new Object(); // use for synchronizing access to primary/overflow channels & drain order

  @GuardedBy(value = "queueLock")
  public ArrayDeque<Event> memQueue;

  private Semaphore memQueRemaining;  // tracks number of free slots in primary channel (includes all active put lists)
                                      // .. used to determine if the puts should go into primary or overflow

  private Semaphore totalStored;     // tracks number of events in both channels. Takes will block on this

  private int maxQueueSize = 0;

  private boolean overflowDisabled;         // if true indicates the overflow should not be used at all.
  private boolean overflowActivated=false;  // indicates if overflow can be used. invariant: false if overflowDisabled is true.

  private int memoryCapacity = Integer.MIN_VALUE;     // max events that the channel can hold  in memory
  private int overflowCapacity;

  private int overflowTimeout;

  private double overflowDeactivationThreshold = defaultOverflowDeactivationThreshold / 100; // mem full % at which we stop spill to overflow

  // memory consumption control
  private static final int defaultAvgEventSize = 100;
  private static final Long defaultByteCapacity = (long)(Runtime.getRuntime().maxMemory() * .80);
  private static final int defaultByteCapacityBufferPercentage = 20;
  private volatile int byteCapacity;
  private volatile double avgEventSize = 100;
  private volatile int lastByteCapacity;
  private volatile int byteCapacityBufferPercentage;
  private Semaphore bytesRemaining;

  public int getMemoryCapacity() {
    return memoryCapacity;
  }
  public int getOverflowTimeout() {
    return overflowTimeout;
  }

  public int getMaxQueueSize() {
    return maxQueueSize;
  }

  protected Integer getOverflowCapacity() {
    return overflowCapacity;
  }

  protected boolean isOverflowDisabled() {
    return overflowDisabled;
  }

  private ChannelCounter channelCounter;

  public DrainOrderQueue drainOrder = new DrainOrderQueue();  // TODO : make private

  public int queueSize() {
    synchronized (queueLock) {
      return memQueue.size();
    }
  }


  private static class MutableInteger {
    private int value;

    public MutableInteger(int val) {
      value = val;
    }

    public void add(int amount) {
      value+=amount;
    }

    public int intValue() {
      return value;
    }
  }

  //  pop on a empty queue will throw NoSuchElementException
  // invariant: 0 will never be left in the queue

  public static class DrainOrderQueue {
    public ArrayDeque<MutableInteger> queue = new ArrayDeque<MutableInteger>(1000);

    public int totalPuts = 0;  // for debugging only
    private long overflowCounter = 0; // number of items currently in overflow channel

    public  String dump() {
      StringBuilder sb = new StringBuilder();

      sb.append("  [ ");
      for(MutableInteger i : queue) {
        sb.append(i.intValue());
        sb.append(" ");
      }
      sb.append("]");
      return  sb.toString();
    }

    public void putPrimary(Integer eventCount) {
      totalPuts+=eventCount;
      if( queue.peekLast()==null || queue.getLast().intValue() < 0)
        queue.addLast(new MutableInteger(eventCount));
      else
        queue.getLast().add(eventCount);
    }

    public void putFirstPrimary(Integer eventCount) {
      if( queue.peekFirst()==null || queue.getFirst().intValue() < 0 )
        queue.addFirst(new MutableInteger(eventCount));
      else
        queue.getFirst().add(eventCount);
    }

    public void putOverflow(Integer eventCount) {
      totalPuts+=eventCount;
      if( queue.peekLast()==null ||  queue.getLast().intValue() > 0  )
        queue.addLast(new MutableInteger(- eventCount));
      else
        queue.getLast().add(-eventCount);
      overflowCounter+=eventCount;
    }

    public void putFirstOverflow(Integer eventCount) {
      if( queue.peekFirst()==null ||  queue.getFirst().intValue() > 0  )
        queue.addFirst(new MutableInteger(-eventCount));
      else
        queue.getFirst().add(-eventCount);
      overflowCounter+=eventCount;
    }

    public int front() {
      return  queue.getFirst().intValue();
    }

    public boolean isEmpty() {
      return queue.isEmpty();
    }

    public void takePrimary(int takeCount) {
      MutableInteger headValue = queue.getFirst();

      if(headValue.intValue() < takeCount)   // this condition is optimization to avoid redundant conversions of int -> Integer -> string in hot path
        Preconditions.checkArgument(headValue.intValue() >= takeCount, "Cannot take {} from {} in DrainOrder Queue", takeCount, headValue.intValue() );

      headValue.add(-takeCount);
      if(headValue.intValue() == 0)
        queue.removeFirst();
    }

    public void takeOverflow(int takeCount) {
      MutableInteger headValue = queue.getFirst();
      Preconditions.checkArgument(headValue.intValue() <= -takeCount, "Cannot take {} from {} in DrainOrder Queue", takeCount, headValue.intValue() );

      headValue.add(takeCount);
      if(headValue.intValue() == 0)
        queue.removeFirst();
      overflowCounter -= takeCount;
    }

  }

  private class SpillableMemoryTransaction extends BasicTransactionSemantics {
    BasicTransactionSemantics overflowTakeTx = null;  // Take-transaction for overflow
    BasicTransactionSemantics overflowPutTx = null;   // Put-transaction for overflow
    boolean useOverflow = false;
    boolean putCalled = false;    // set on first invocation to put
    boolean takeCalled = false;   // set on first invocation to take
    int largestTakeTxSize = 50;
    int largestPutTxSize = 50;

    Integer overflowPutCount = 0;    // puts going to overflow in this transaction

    private int putListByteCount = 0;
    private int takeListByteCount = 0;

    ArrayDeque<Event> takeList;
    ArrayDeque<Event> putList;
    private final ChannelCounter channelCounter;


    public SpillableMemoryTransaction(ChannelCounter counter) {
      takeList = new ArrayDeque<Event>(largestTakeTxSize);
      putList = new ArrayDeque<Event>(largestPutTxSize);
      channelCounter = counter;
    }

    private void debug(String msg) {
//      System.out.println("********  " + msg);
    }


    @Override
    public void begin() {
      super.begin();
    }

    @Override
    public void close() {
      if(overflowTakeTx!=null)
        overflowTakeTx.close();
      if(overflowPutTx!=null)
        overflowPutTx.close();
      super.close();
    }


    @Override
    protected void doPut(Event event) throws InterruptedException {
      channelCounter.incrementEventPutAttemptCount();

      putCalled = true;
      int eventByteSize = (int)Math.ceil(estimateEventSize(event)/ avgEventSize);
      if(! putList.offer(event)) {
        throw new ChannelFullException("Put queue for SpillableMemoryTransaction of capacity " +
                putList.size() + " full, consider committing more frequently, " +
                "increasing capacity or increasing thread count");
      }
      putListByteCount += eventByteSize;
    }


    // Take will limit itself to a single channel within a transaction. This ensures commits/rollbacks
    //  are restricted to a single channel.
    @Override
    protected Event doTake() throws InterruptedException {
//      debug("Taking() ");
      channelCounter.incrementEventTakeAttemptCount();

      if(!totalStored.tryAcquire(overflowTimeout, TimeUnit.SECONDS)) {
//        debug("Take is backing off! ");
        return null;
      }

      Event event;
      synchronized(queueLock) {

        int drainOrderTop = drainOrder.front();

        if(!takeCalled) {
          takeCalled = true;
          if( drainOrderTop < 0 ) {
            useOverflow = true;
            overflowTakeTx = getOverflowTx();
            overflowTakeTx.begin();
          }
        }

        if( useOverflow ) {
          if(drainOrderTop > 0) {
            debug("Take is switching to primary! ");
            totalStored.release();
            return null;       // takes should now occur from primary channel
          }

          event = overflowTakeTx.take();
          drainOrder.takeOverflow(1);
        } else {
          if( drainOrderTop < 0 ) {
            debug("Take is switching to overflow! ");
            totalStored.release();
            return null;      // takes should now occur from overflow channel
          }

          event =  memQueue.poll();
          drainOrder.takePrimary(1);
          Preconditions.checkNotNull(event, "Queue.poll returned NULL despite semaphore " +
                  "signalling existence of entry");
//          debug("Take  " + EventHelper.dumpEvent(event));
        }
      }

      if(!useOverflow) {
        takeList.offer(event);  // does not need to happen inside synchronized block
      }

      int eventByteSize = (int)Math.ceil(estimateEventSize(event)/ avgEventSize);
      takeListByteCount += eventByteSize;
      return event;
    }

    private BasicTransactionSemantics getOverflowTx() {
      return newFileBackedTransaction();
    }

    @Override
    protected void doCommit() throws InterruptedException {
//      debug("Committing()");
//      System.out.println(drainOrder.dump());
      if(putCalled)
        putCommit();
      else if(takeCalled)
        takeCommit();
    }

    private void takeCommit() {
      if(takeList.size() > largestTakeTxSize)
        largestTakeTxSize = takeList.size();

      synchronized (queueLock) {
        if(overflowTakeTx!=null) {
          overflowTakeTx.commit();
        }
        double memoryPercentFree = (memoryCapacity==0)
                             ?  0
                             :  (memoryCapacity - memQueue.size() + takeList.size() ) / (double)memoryCapacity ;
        if(overflowActivated  &&  memoryPercentFree >= overflowDeactivationThreshold ) {
          overflowActivated = false;
//          System.out.println("OVERFLOW DEACTIVATED");
        }
      }
      if(!useOverflow)  {
        memQueRemaining.release(takeList.size());
        bytesRemaining.release(takeListByteCount);
      }

      channelCounter.addToEventTakeSuccessCount(takeList.size());
    }

    private void putCommit() throws InterruptedException {
      // decide if overflow needs to be used
      int timeout = overflowActivated  ? 0 : overflowTimeout;

      if( memoryCapacity!=0 ) {
          // check if we have enough event slots(memoryCapacity) for using memory queue
          if( !memQueRemaining.tryAcquire(putList.size(), timeout, TimeUnit.SECONDS) ) {
              if(overflowDisabled)
                  throw new ChannelFullException("Spillable Memory Channel's memory capacity has been " +
                          "reached and overflow is disabled. Consider increasing memoryCapacity.");
              overflowActivated = true;
              useOverflow = true;
          }
          // check if we have enough byteCapacity for using memory queue
          else if( !bytesRemaining.tryAcquire(putListByteCount, overflowTimeout, TimeUnit.SECONDS) )  {
              memQueRemaining.release(putList.size());
              if(overflowDisabled)
                  throw new ChannelFullException("Spillable Memory Channel's memory capacity has been reached.  "
                          + (bytesRemaining.availablePermits() * (int) avgEventSize) + " bytes are free "
                          + "and overflow is disabled. Consider increasing byteCapacity or capacity." );
              overflowActivated = true;
              useOverflow = true;
  //            System.out.println("USING OVERFLOW");
          }
      }

      if(putList.size() > largestPutTxSize)
        largestPutTxSize = putList.size();

      if (useOverflow) {
        overflowPutTx = getOverflowTx();
        overflowPutTx.begin();
        for(Event event : putList) {
          overflowPutTx.put(event);
        }
        overflowPutCommit(overflowPutTx);
        totalStored.release(putList.size());
        overflowPutCount+= putList.size();
        return;
      } else {
        synchronized (queueLock) {
          for(Event e : putList ) {
  //            debug("Putt  " + EventHelper.dumpEvent(e));
            if(!memQueue.offer(e)) {
              throw new ChannelException("Unable to add to in memory queue, this is very unexpected");
            }
          }
          drainOrder.putPrimary(putList.size());
          maxQueueSize = (memQueue.size() > maxQueueSize) ? memQueue.size() : maxQueueSize;
        }
        // update counters and semaphores
        totalStored.release(putList.size());
        channelCounter.addToEventPutSuccessCount(putList.size());
      }
    }

    private void overflowPutCommit(Transaction overflowTx) throws InterruptedException {
      for(int i=0; i<2; ++i)  {  // reattempt only once if overflow is full first time around
        try {
          synchronized(queueLock) {
            overflowTx.commit();
            drainOrder.putOverflow(putList.size());
            break;
          }
        } catch (ChannelFullException e)  { // drop lock & reattempt
          if(i==0) {
            System.out.println("Sleeping");
            Thread.sleep(overflowTimeout *1000);
          }
          else
            throw e;
        }
      }
    }

    @Override
    protected void doRollback() {
//      debug("Rollback()" + (takeCalled ? " take" : (putCalled ? " put" : "N/A")));

      if(putCalled) {
        if(overflowPutTx!=null)
          overflowPutTx.rollback();
        if(!useOverflow) {
          bytesRemaining.release(putListByteCount);
          // putList.clear()
        }
        // putListByteCount = 0;
      }
      else if(takeCalled) {
        int takeCount = takeList.size();
        synchronized(queueLock) {
          if(overflowTakeTx!=null)
            overflowTakeTx.rollback();
          if(useOverflow) {
            drainOrder.putFirstOverflow(takeList.size());
          } else {
            int remainingCapacity = memoryCapacity - memQueue.size();
            Preconditions.checkState(remainingCapacity >= takeList.size(), "Not enough space in memory " +
                    "queue to rollback takes. This should never happen, please report");
            while(!takeList.isEmpty()) {
              memQueue.addFirst(takeList.removeLast());
            }
            drainOrder.putFirstPrimary(takeCount);
          }
        }
        if(!useOverflow)
          totalStored.release(takeCount);
        // takeListByteCount = 0;
      } else {
        overflowTakeTx.rollback();
      }
      channelCounter.setChannelSize(memQueue.size() + getDepth() );
    }
  } // Transaction


  public SpillableMemoryChannel() {
    super();
  }

  /**
   * Read parameters from context
   * <li>memoryCapacity = total number of events allowed at one time in the memory queue.
   * <li>overflowCapacity = total number of events allowed at one time in the overflow file channel.
   * <li>byteCapacity = the max number of bytes used for events in the queue.
   * <li>byteCapacityBufferPercentage = type int. Defines the percent of buffer between byteCapacity and the estimated event size.
   * <li>overflowTimeout = type int. Number of seconds to wait on a full memory before deciding to enable overflow
   */
  @Override
  public void configure(Context context) {

    if(totalStored==null)
      totalStored = new Semaphore(0);

    if (channelCounter == null)
      channelCounter = new ChannelCounter(getName());

    // 1) Memory Capacity
    Integer newMemoryCapacity;
    try {
      newMemoryCapacity = context.getInteger("memoryCapacity", defaultMemoryCapacity);
      if(newMemoryCapacity==null)
        newMemoryCapacity = defaultMemoryCapacity;
      if(newMemoryCapacity<0)
        throw new NumberFormatException("memoryCapacity must be >= 0");

    } catch(NumberFormatException e) {
      newMemoryCapacity = defaultMemoryCapacity;
      LOGGER.warn("Invalid memoryCapacity specified, initializing channel to "
                + "default value of {}", defaultMemoryCapacity);
    }
    try {
      resizePrimaryQueue(newMemoryCapacity);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // overflowTimeout - how long to wait before switching to overflow when mem is full
    try {
      Integer newOverflowTimeout = context.getInteger("overflowTimeout", defaultOverflowTimeout);
      overflowTimeout = (newOverflowTimeout != null) ? newOverflowTimeout : defaultOverflowTimeout;
    } catch(NumberFormatException e) {
      LOGGER.warn("Incorrect specification for SpillableMemoryChannel's keep-alive." +
              " Using default value {}", defaultOverflowTimeout);
      overflowTimeout = defaultOverflowTimeout;
    }

    // overflowDeactivationThreshold -- For internal use. To remain undocumented.
    // determines the % free space in memory queue at which we stop spilling to overflow
    try {
      Integer newThreshold = context.getInteger("overflowDeactivationThreshold");
      overflowDeactivationThreshold =  (newThreshold != null) ? newThreshold/100.0  : defaultOverflowDeactivationThreshold / 100.0;
    } catch(NumberFormatException e) {
      LOGGER.warn("Incorrect specification for SpillableMemoryChannel's overflowDeactivationThreshold." +
              " Using default value {} %", defaultOverflowDeactivationThreshold);
      overflowDeactivationThreshold = defaultOverflowDeactivationThreshold / 100.0;
    }

    // 3) Memory consumption control

    try {
      byteCapacityBufferPercentage = context.getInteger("byteCapacityBufferPercentage", defaultByteCapacityBufferPercentage);
    } catch(NumberFormatException e) {
      byteCapacityBufferPercentage = defaultByteCapacityBufferPercentage;
    }

    try { // avgEventSize .. to remain undocumented.. used in unit testing
      avgEventSize = context.getInteger("avgEventSize", defaultAvgEventSize);
    } catch ( NumberFormatException e) {
      avgEventSize = defaultAvgEventSize;
    }

    try {
      byteCapacity = (int)((context.getLong("byteCapacity", defaultByteCapacity) * (1 - byteCapacityBufferPercentage * .01 )) / avgEventSize);
      if (byteCapacity < 1) {
        byteCapacity = Integer.MAX_VALUE;
      }
    } catch(NumberFormatException e) {
      byteCapacity = (int)((defaultByteCapacity * (1 - byteCapacityBufferPercentage * .01 )) / avgEventSize);
    }


    if (bytesRemaining == null) {
      bytesRemaining = new Semaphore(byteCapacity);
      lastByteCapacity = byteCapacity;
    } else {
      if (byteCapacity > lastByteCapacity) {
        bytesRemaining.release(byteCapacity - lastByteCapacity);
        lastByteCapacity = byteCapacity;
      } else {
        try {
          if(!bytesRemaining.tryAcquire(lastByteCapacity - byteCapacity, overflowTimeout, TimeUnit.SECONDS)) {
            LOGGER.warn("Couldn't acquire permits to downsize the byte capacity, resizing has been aborted");
          } else {
            lastByteCapacity = byteCapacity;
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }

    try {
      overflowCapacity = context.getInteger("overflowCapacity", defaultOverflowCapacity);  // file channel capacity
      // Determine if File channel needs to be disabled
        overflowDisabled = (overflowCapacity<1) ;
        if(overflowDisabled)
          overflowActivated = false;
    } catch(NumberFormatException e) {
      overflowCapacity = defaultOverflowCapacity;
    }



    // Configure File channel
    context.put("keep-alive","0"); // override keep-alive for  File channel
    context.put("capacity", Integer.toString(overflowCapacity) );  // file channel capacity
    super.configure(context);
  }


  private void resizePrimaryQueue(int newMemoryCapacity) throws InterruptedException {
    if(memQueue != null   &&   memoryCapacity == newMemoryCapacity)
      return;

    if (memoryCapacity > newMemoryCapacity) {
      int diff = memoryCapacity - newMemoryCapacity;
      if(!memQueRemaining.tryAcquire(diff, overflowTimeout, TimeUnit.SECONDS)) {
        LOGGER.warn("Memory buffer currently contains more events than the new size. Downsizing has been aborted.");
        return;
      }
      synchronized(queueLock) {
        ArrayDeque<Event> newQueue = new ArrayDeque<Event>(newMemoryCapacity);
        newQueue.addAll(memQueue);
        memQueue = newQueue;
        memoryCapacity = newMemoryCapacity;
      }
    } else  {   // if (memoryCapacity <= newMemoryCapacity)
      synchronized(queueLock) {
        ArrayDeque<Event> newQueue = new ArrayDeque<Event>(newMemoryCapacity);
        if(memQueue !=null)
          newQueue.addAll(memQueue);
        memQueue = newQueue;
        if(memQueRemaining == null ) {
          memQueRemaining = new Semaphore(newMemoryCapacity);
        } else {
          int diff = newMemoryCapacity - memoryCapacity;
          memQueRemaining.release(diff);
        }
        memoryCapacity = newMemoryCapacity;
      }
    }
  }

  @Override
  public synchronized void start() {
    super.start();
    int overFlowCount = super.getDepth();
    if(drainOrder.isEmpty()) {
      drainOrder.putOverflow(overFlowCount);
      totalStored.release(overFlowCount);
    }
    int totalCount =  overFlowCount + memQueue.size();
    channelCounter.setChannelCapacity(memoryCapacity + getOverflowCapacity());
    channelCounter.setChannelSize( totalCount );
  }

  @Override
  public synchronized void stop() {
    channelCounter.setChannelSize(getDepth() + memQueue.size());
    channelCounter.stop();
    super.stop();
  }

  @Override
  protected BasicTransactionSemantics createTransaction() {
    return new SpillableMemoryTransaction(channelCounter);
  }

  private long estimateEventSize(Event event)
  {
    byte[] body = event.getBody();
    if(body != null && body.length != 0) {
      return body.length;
    }
    //Each event occupies at least 1 slot, so return 1.
    return 1;
  }

}



