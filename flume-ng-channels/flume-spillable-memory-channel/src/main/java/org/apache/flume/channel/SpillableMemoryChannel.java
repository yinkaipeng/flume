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
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.GuardedBy;
import org.apache.flume.annotations.Recyclable;

import org.apache.flume.ChannelException;
import org.apache.flume.ChannelFullException;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.instrumentation.ChannelCounter;

import org.mortbay.log.Log;
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
public class SpillableMemoryChannel extends BasicChannelSemantics {
  private static Logger LOGGER = LoggerFactory.getLogger(SpillableMemoryChannel.class);
  public static final Integer defaultMemoryCapacity = 100;
  public static final Integer totalCapacityUnlimited = Integer.MAX_VALUE;  // Unlimited

  public static final Integer defaultMaxTransactionBatchSize = 100;
  private static final double byteCapacitySlotSize = 100;
//  public static final Long defaultByteCapacity = (long)(Runtime.getRuntime().maxMemory() * .80);
  private static final Integer defaultByteCapacityBufferPercentage = 20;

  public static final Integer defaultKeepAlive = 3;
  public static final Integer defaultOverflowDeactivationThreshold = 5; // percent

  private Object queueLock = new Object(); // use for synchronizing access to primary/overflow channels & drain order

  @GuardedBy(value = "queueLock")
//  private LinkedBlockingDeque<Event> queue;
  public ArrayDeque<Event> queue;

  private Semaphore queueRemaining;  // tracks number of free slots in primary channel (includes all active put lists)
                                     // .. used to determine if the puts should go into primary or overflow

  private Semaphore totalRemaining;  // tracks number of free slots in primary + overflow. Puts will block on this.
  private Semaphore totalStored;     // tracks number of events in both channels. Takes will block on this

  private int maxQueueSize = 0;

  private boolean overflowDisabled;  // if true indicates the overflow should not be used at all.
  private boolean overflowActivated=false;  // indicates if overflow can be used. invariant: false if overflowDisabled is true.

//  private Semaphore bytesRemaining;

  private int transBatchSize;  // largest batch size that the transaction can accomodate
  private int totalCapacity = 0;     // max events that the channel can hold (memory + overflow)
  private int memoryCapacity = Integer.MIN_VALUE;     // max events that the channel can hold  in memory

  private int keepAlive;

  private double overflowDeactivationThreshold = defaultOverflowDeactivationThreshold / 100; // mem full % at which we stop spill to overflow

  public int getTransBatchSize() {
    return transBatchSize;
  }
  public int getTotalCapacity() {
    return totalCapacity;
  }
  public int getMemoryCapacity() {
    return memoryCapacity;
  }
  public int getKeepAlive() {
    return keepAlive;
  }

  public int getMaxQueueSize() {
    return maxQueueSize;
  }

//  private volatile int byteCapacity;
//  private volatile int lastByteCapacity;
//  private volatile int byteCapacityBufferPercentage;

  private String overflowChannelName;
  private BasicChannelSemantics overflowChannel;
  private BasicChannelSemantics overflowChannelTmp;

  private ChannelCounter channelCounter;

  public DrainOrderQueue drainOrder = new DrainOrderQueue();  // TODO : make private

  public int queueSize() {
    synchronized (queueLock) {
      return queue.size();
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
//    public ArrayDeque<AtomicInteger> queue = new ArrayDeque<AtomicInteger>(1000);
    public ArrayDeque<MutableInteger> queue = new ArrayDeque<MutableInteger>(1000);

    public int totalPuts = 0;  // TODO: remove, for debugging only
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
    BasicTransactionSemantics overflowTransaction;  // transaction object for overflow channel
    boolean useOverflow = false;
    boolean putCalled = false;    // set on first invocation to put
    boolean takeCalled = false;   // set on first invocation to take

    Integer takeCount = 0;           // total Takes in this transaction
    Integer overflowPutCount = 0;    // puts going to overflow in this transaction

    private int putByteCounter = 0;
    private int takeByteCounter = 0;

    ArrayDeque<Event> takeList;
    ArrayDeque<Event> putList;
    private final ChannelCounter channelCounter;


    public SpillableMemoryTransaction(Integer transBatchSize, ChannelCounter counter) {
      overflowTransaction = overflowChannel.createTransaction();
      takeList = new ArrayDeque<Event>(transBatchSize);
      putList = new ArrayDeque<Event>(transBatchSize);
      channelCounter = counter;
    }

    private void debug(String msg) {
//      System.out.println(msg);
    }


    @Override
    public void begin() {
      overflowTransaction.begin();
      super.begin();
    }

    @Override
    public void close() {
      overflowTransaction.close();
      super.close();
    }


    @Override
    protected void doPut(Event event) throws InterruptedException {
      channelCounter.incrementEventPutAttemptCount();

      if( putList.size() >= transBatchSize )
        throw new ChannelException("Put queue for SpillableMemoryTransaction is full. Consider increasing the maxTransactionBatchSize " +
                " to match the largest batchSize of the source/sink");

// TODO : see what to do with this semaphore      if( bytesRemaining.tryAcquire(putByteCounter) )
      if(!putCalled) {
        putCalled = true;
        int timeout = overflowActivated  ? 0 :  keepAlive;

        if( !queueRemaining.tryAcquire(transBatchSize, timeout, TimeUnit.SECONDS) ) {
          if(overflowDisabled)
            throw new ChannelFullException("SpillableMemory Channel's memory capacity has been reached and overflow channel is not used. " +
                    "Consider increasing the memoryCapacity or totalCapacity.");
          overflowActivated = true;
          useOverflow = true;
//          System.out.println("USING OVERFLOW");
        }
      }

      if (useOverflow) {
        overflowTransaction.put(event);
        ++overflowPutCount;
        return;
      }

      int eventByteSize = (int)Math.ceil(estimateEventSize(event)/byteCapacitySlotSize);
 //     if (bytesRemaining.tryAcquire(eventByteSize, keepAlive, TimeUnit.SECONDS)) {
//        if(putList.size() >= transBatchSize ) {
//          throw new ChannelException("Put queue for SpillableMemoryTransaction of capacity " +
//                  putList.size() + " full, consider committing more frequently, " +
//                  "increasing capacity or increasing thread count");
//        }
        if(! putList.offer(event)) {
          throw new ChannelFullException("Put queue for SpillableMemoryTransaction of capacity " +
                  putList.size() + " full, consider committing more frequently, " +
                  "increasing capacity or increasing thread count");
        }
//      } else {
//        throw new ChannelException("Put queue for SpillableMemoryTransaction of byteCapacity " +
//                (lastByteCapacity * (int)byteCapacitySlotSize) + " bytes cannot add an " +
//                " event of size " + estimateEventSize(event) + " bytes because " +
//                (bytesRemaining.availablePermits() * (int)byteCapacitySlotSize) + " bytes are already used." +
//                " Try consider committing more frequently, increasing byteCapacity or increasing thread count");
//      }
      putByteCounter += eventByteSize;
    }


    // Take will limit itself to a single channel within a transaction. This ensures commits/rollbacks
    //  are restricted to a single channel.
    @Override
    protected Event doTake() throws InterruptedException {
      debug("Taking() ");
      channelCounter.incrementEventTakeAttemptCount();

      if(takeList.size() > transBatchSize )
        throw new ChannelException("Take queue for SpillableMemoryTransaction is full. Consider increasing the maxTransactionBatchSize " +
                " to match the largest batchSize of the source/sink");

      if(!totalStored.tryAcquire(keepAlive, TimeUnit.SECONDS)) {
        debug("Take is backing off! ");
        return null;
      }

      Event event;
      synchronized(queueLock) {

        int drainOrderTop = drainOrder.front();

        if(!takeCalled) {
          takeCalled = true;
          useOverflow = ( drainOrderTop < 0 );
        }

        if( useOverflow ) {
          if(drainOrderTop > 0) {
            debug("Take is switching to primary! ");
            totalStored.release();
            return null;       // takes should now occur from primary channel
          }

          event = overflowTransaction.take();
          drainOrder.takeOverflow(1);
        } else {
          if( drainOrderTop < 0 ) {
            debug("Take is switching to overflow! ");
            totalStored.release();
            return null;      // takes should now occur from overflow channel
          }

          if( takeList.size() == transBatchSize) {
            totalStored.release();
            throw new ChannelException("TakeList for SpillableMemory Channel's transaction is full." +
                    " Consider reducing sink's batch size, or increasing increasing the channel's maxTransactionBatchSize");
          }

          event =  queue.poll();
          drainOrder.takePrimary(1);
          Preconditions.checkNotNull(event, "Queue.poll returned NULL despite semaphore " +
                  "signalling existence of entry");
//          debug("Take  " + EventHelper.dumpEvent(event));
        }
        ++takeCount;
      }

      if(!useOverflow) {
        takeList.offer(event);  // does not need to happen inside synchronized block
      }

      int eventByteSize = (int)Math.ceil(estimateEventSize(event)/byteCapacitySlotSize);
      takeByteCounter += eventByteSize;
      return event;
    }

    @Override
    protected void doCommit() throws InterruptedException {
      debug("Committing()");
      if(putCalled)
        putCommit();
      else if(takeCalled)
        takeCommit();
      else {
        synchronized (queueLock) {
          overflowTransaction.commit();
        }
      }
    }

    private void takeCommit() {
      synchronized (queueLock) {
        overflowTransaction.commit();
        double memoryPercentFree = (memoryCapacity==0)
                             ?  0
                             :  (memoryCapacity - queue.size() + transBatchSize) / (double)memoryCapacity ;
        if(overflowActivated  &&  memoryPercentFree >= overflowDeactivationThreshold ) {
          overflowActivated = false;
//          System.out.println("OVERFLOW DEACTIVATED");
          }
        }
      if(!useOverflow)
        queueRemaining.release(takeCount);
      totalRemaining.release(takeCount);
      //TODO ::  bytesRemaining.release(takeByteCounter);
      channelCounter.addToEventTakeSuccessCount(takeCount);
    }

    private void putCommit() throws InterruptedException {
      int puts = (useOverflow) ? overflowPutCount :  putList.size();

      if(!totalRemaining.tryAcquire(puts,keepAlive, TimeUnit.SECONDS)) {
        if( totalCapacity == totalCapacityUnlimited ) {
          totalRemaining.release(totalCapacityUnlimited);
        } else {
          throw new ChannelFullException("SpillableMemory Channel is full. Sinks have probably" +
                " not been able to keep up. Try increasing totalCapacity or restore sink.");
        }
      }

      if(useOverflow) {
        for(int i=0; i<2; ++i)  {  // reattempt only once if overflow is full first time around
          try {
            synchronized(queueLock) {
                overflowTransaction.commit();
                drainOrder.putOverflow(puts);
                break;
              }
          } catch (ChannelFullException e)  { // drop lock & reattempt
            if(i==0) {
              System.out.println("Sleeping");
              Thread.sleep(keepAlive*1000);
            }
            else
              throw e;
          }
        }
        totalStored.release(puts);
      } else {
        synchronized (queueLock) {
          while(!putList.isEmpty()) {
            Event e = putList.removeFirst();
//            debug("Putt  " + EventHelper.dumpEvent(e));
            if(!queue.offer(e)) {
              throw new ChannelException("Unable to add to in memory queue, this is very unexpected");
            }
          }
          drainOrder.putPrimary(puts);
          overflowTransaction.commit(); // need to commit overflow even if not used
          maxQueueSize = (queue.size() > maxQueueSize) ? queue.size() : maxQueueSize;
        }
        queueRemaining.release(transBatchSize - puts); // return the extra space reserved
        totalStored.release(puts);
        channelCounter.addToEventPutSuccessCount(puts);
      }
    }

    @Override
    protected void doRollback() {
//      debug("Rollback()" + (takeCalled ? " take" : (putCalled ? " put" : "N/A")));

      if(putCalled) {
        overflowTransaction.rollback();
        if(!useOverflow) {
          //TODO : bytesRemaining.release(putByteCounter);
          // putList.clear()
        }
        // putByteCounter = 0;
      }
      else if(takeCalled) {
        synchronized(queueLock) {
          overflowTransaction.rollback();
          if(useOverflow) {
            drainOrder.putFirstOverflow(takeCount);
          } else {
            int remainingCapacity = memoryCapacity - queue.size();
            Preconditions.checkState(remainingCapacity >= takeList.size(), "Not enough space in memory channel " +
                    "queue to rollback takes. This should never happen, please report");
            while(!takeList.isEmpty()) {
              queue.addFirst(takeList.removeLast());
            }
            drainOrder.putFirstPrimary(takeCount);
          }
        }
        if(!useOverflow)
          totalStored.release(takeCount);
        // takeByteCounter = 0;
      } else {
        overflowTransaction.rollback();
      }
      channelCounter.setChannelSize(queue.size()); // TODO: Probably need to add the size of the overflow channel's queue also
    }
  } // Transaction


  public SpillableMemoryChannel() {
    super();
  }

  /**
   * Read parameters from context
   * <li>capacity = type long that defines the total number of events allowed at one time in the queue.
   * <li>maxTransactionBatchSize = type long that defines the total number of events allowed in one transaction.
   * <li>byteCapacity = type long that defines the max number of bytes used for events in the queue.
   * <li>byteCapacityBufferPercentage = type int that defines the percent of buffer between byteCapacity and the estimated event size.
   * <li>keep-alive = type int that defines the number of second to wait for a queue permit
   */
  @Override
  public void configure(Context context) {

    String newOverflowChannelName = context.getString("overflowChannel");
    Preconditions.checkNotNull(newOverflowChannelName, "overflowChannel configuration not specified for SpillableChannel");
    if( overflowChannelName==null )
      overflowChannelName = newOverflowChannelName;

    else if(overflowChannelName.compareTo(newOverflowChannelName)!=0 ) {
      //TODO ... handling rename of overflow on reconfiguration ? renaming is ok .. but different instance NOT
    }

    if(totalStored==null)
      totalStored = new Semaphore(0);

    if (channelCounter == null) {
      channelCounter = new ChannelCounter(getName());
    }

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


    // 2) totalCapacity
    try {
      Integer newTotalCapacity = context.getInteger("totalCapacity", totalCapacityUnlimited);
      if(newTotalCapacity==null)
        newTotalCapacity = totalCapacityUnlimited;
      if(newTotalCapacity<=0)
        throw new NumberFormatException("totalCapacity must be a positive number");
      Preconditions.checkState(newTotalCapacity >= memoryCapacity,
              "totalCapacity of Spillable Channel cannot be less than memoryCapacity");
      resizeTotalCapacity(newTotalCapacity);
    } catch(NumberFormatException e) {
      totalCapacity = totalCapacityUnlimited;
      LOGGER.warn("Invalid totalCapacity specified, initializing channel to "
              + "default value of {}", totalCapacityUnlimited);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    synchronized (queueLock) {
      overflowDisabled =  (totalCapacity == memoryCapacity ) ;
      if(overflowDisabled)
        overflowActivated = false;
    }


    // 3) maxTransactionBatchSize
    try {
      Integer newTransBatchSize = context.getInteger("maxTransactionBatchSize", defaultMaxTransactionBatchSize);
      transBatchSize = (newTransBatchSize==null) ? defaultMaxTransactionBatchSize : newTransBatchSize;
      if (transBatchSize <= 0) {
        throw new NumberFormatException("maxTransactionBatchSize must be a positive number");
      }

//      Preconditions.checkState(transBatchSize <= memoryCapacity,
//              "maxTransactionBatchSize for SpillableMemory Channel cannot be greater than memoryCapacity");
      Preconditions.checkState(transBatchSize <= totalCapacity,
              "maxTransactionBatchSize for SpillableMemory Channel cannot be greater than totalCapacity");

    } catch(NumberFormatException e) {
      transBatchSize = defaultMaxTransactionBatchSize;
      LOGGER.warn("Invalid maxTransactionBatchSize specified, initializing SpillableMemory Channel"
              + " to default value of {}", defaultMaxTransactionBatchSize);
    }

//    try {
//      byteCapacityBufferPercentage = context.getInteger("byteCapacityBufferPercentage", defaultByteCapacityBufferPercentage);
//    } catch(NumberFormatException e) {
//      byteCapacityBufferPercentage = defaultByteCapacityBufferPercentage;
//    }
//
//    try {
//      byteCapacity = (int)((context.getLong("byteCapacity", defaultByteCapacity) * (1 - byteCapacityBufferPercentage * .01 )) /byteCapacitySlotSize);
//      if (byteCapacity < 1) {
//        byteCapacity = Integer.MAX_VALUE;
//      }
//    } catch(NumberFormatException e) {
//      byteCapacity = (int)((defaultByteCapacity * (1 - byteCapacityBufferPercentage * .01 )) /byteCapacitySlotSize);
//    }

    // keep-alive
    try {
      Integer newKeepAlive = context.getInteger("keep-alive", defaultKeepAlive);
      keepAlive = (newKeepAlive != null) ? newKeepAlive : defaultKeepAlive;
    } catch(NumberFormatException e) {
      LOGGER.warn("Incorrect specification for SpillableMemoryChannel's keep-alive." +
              " Using default value {}", defaultKeepAlive);
      keepAlive = defaultKeepAlive;
    }

    // overflowDeactivationThreshold -- For internal use. To remain undocumented .
    try {
      Integer newThreshold = context.getInteger("overflowDeactivationThreshold");
      overflowDeactivationThreshold =  (newThreshold != null) ? newThreshold/100.0  : defaultOverflowDeactivationThreshold / 100.0;
    } catch(NumberFormatException e) {
      LOGGER.warn("Incorrect specification for SpillableMemoryChannel's overflowDeactivationThreshold." +
              " Using default value {} %", defaultOverflowDeactivationThreshold);
      overflowDeactivationThreshold = defaultOverflowDeactivationThreshold / 100.0;
    }

//    if (bytesRemaining == null) {
//      bytesRemaining = new Semaphore(byteCapacity);
//      lastByteCapacity = byteCapacity;
//    } else {
//      if (byteCapacity > lastByteCapacity) {
//        bytesRemaining.release(byteCapacity - lastByteCapacity);
//        lastByteCapacity = byteCapacity;
//      } else {
//        try {
//          if(!bytesRemaining.tryAcquire(lastByteCapacity - byteCapacity, keepAlive, TimeUnit.SECONDS)) {
//            LOGGER.warn("Couldn't acquire permits to downsize the byte capacity, resizing has been aborted");
//          } else {
//            lastByteCapacity = byteCapacity;
//          }
//        } catch (InterruptedException e) {
//          Thread.currentThread().interrupt();
//        }
//      }
//    }

  }

  /**
   * Read parameters from context
   * <li>capacity = type long that defines the total number of events allowed at one time in the queue.
   * <li>maxTransactionBatchSize = type long that defines the total number of events allowed in one transaction.
   * <li>byteCapacity = type long that defines the max number of bytes used for events in the queue.
   * <li>byteCapacityBufferPercentage = type int that defines the percent of buffer between byteCapacity and the estimated event size.
   * <li>keep-alive = type int that defines the number of second to wait for a queue permit
   */
  @Override
  public void postConfigure(Map<String, AbstractChannel> channelMap) {
    overflowChannelTmp  =  (BasicChannelSemantics) channelMap.get(overflowChannelName);
    Preconditions.checkState(overflowChannelTmp!=null, "Channel '" + overflowChannelName + "' not found");
  }

  private void resizeTotalCapacity(int newTotalCapacity) throws InterruptedException {
    if(newTotalCapacity == totalCapacity)
      return;
    if( totalRemaining == null ) {
      totalRemaining = new Semaphore(newTotalCapacity);
    } else  if( totalCapacity > newTotalCapacity ) {  // downsize
      int diff = totalCapacity - newTotalCapacity;
      if( !totalRemaining.tryAcquire(diff, keepAlive, TimeUnit.SECONDS) )
        Log.warn("Spillable Channel currently has more events stored in it than the new total capacity. Aborting downsize attempt.");
    } else {                                          // upsize
      int diff =  newTotalCapacity - totalCapacity;
      totalRemaining.release(diff);
    }
    totalCapacity = newTotalCapacity;
  }

  private void resizePrimaryQueue(int newMemoryCapacity) throws InterruptedException {
    if(queue != null   &&   memoryCapacity == newMemoryCapacity)
      return;

//    if(queue != null) {
//      try {
//        resizePrimaryQueue(memoryCapacity);
//      } catch (InterruptedException e) {
//        Thread.currentThread().interrupt();
//      }
//    } else {
//      synchronized(queueLock) {
//        queue = new LinkedBlockingDeque<Event>(memoryCapacity);
//        queueRemaining = new Semaphore(memoryCapacity);
//        totalStored = new Semaphore(0);
//        totalRemaining = new Semaphore(totalCapacity);
//      }
//    }

    if (memoryCapacity > newMemoryCapacity) {
      int diff = memoryCapacity - newMemoryCapacity;
      if(!queueRemaining.tryAcquire(diff, keepAlive, TimeUnit.SECONDS)) {
        LOGGER.warn("Memory buffer currently contains more events than the new size. Downsizing has been aborted.");
        return;
      }
      synchronized(queueLock) {
//        LinkedBlockingDeque<Event> newQueue = new LinkedBlockingDeque<Event>(newMemoryCapacity);
        ArrayDeque<Event> newQueue = new ArrayDeque<Event>(newMemoryCapacity);
        newQueue.addAll(queue);
        queue = newQueue;
        memoryCapacity = newMemoryCapacity;
      }
    } else  {   // if (memoryCapacity <= newMemoryCapacity)
      synchronized(queueLock) {
//        LinkedBlockingDeque<Event> newQueue = new LinkedBlockingDeque<Event>(newMemoryCapacity);
        ArrayDeque<Event> newQueue = new ArrayDeque<Event>(newMemoryCapacity);
        if(queue!=null)
          newQueue.addAll(queue);
        queue = newQueue;
        if(queueRemaining == null ) {
          queueRemaining = new Semaphore(newMemoryCapacity);
        } else {
          int diff = newMemoryCapacity - memoryCapacity;
          queueRemaining.release(diff);
        }
        memoryCapacity = newMemoryCapacity;
      }
    }
  }

  @Override
  public synchronized void start() {
    channelCounter.start();
    channelCounter.setChannelSize(queue.size());
    int remainingCapacity = memoryCapacity - queue.size();
    channelCounter.setChannelCapacity(queue.size() + remainingCapacity);
    overflowChannel = overflowChannelTmp;
    overflowChannel.start();
    super.start();
  }

  @Override
  public synchronized void stop() {
    channelCounter.setChannelSize(queue.size());
    channelCounter.stop();
    overflowChannel.stop();
    super.stop();
  }

  @Override
  protected BasicTransactionSemantics createTransaction() {
    return new SpillableMemoryTransaction(transBatchSize, channelCounter);
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



