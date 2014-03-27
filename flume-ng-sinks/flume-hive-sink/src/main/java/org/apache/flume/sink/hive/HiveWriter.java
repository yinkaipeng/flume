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

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hive.streaming.*;

import org.apache.flume.Event;

import org.apache.flume.instrumentation.SinkCounter;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Internal API intended for HiveSink use.
 */
class HiveWriter {

  private static final Logger LOG = LoggerFactory
      .getLogger(HiveWriter.class);

  private final HiveEndPoint endPoint;
  private HiveEventSerializer serializer;
  private final StreamingConnection connection;
  private final int txnsPerBatch;
  private final RecordWriter recordWriter;
  private TransactionBatch txnBatch;

  private final ExecutorService callTimeoutPool;

  private final long callTimeout;
  private volatile ScheduledFuture<Void> idleFuture;

  private long lastUsed; // time of last flush on this writer

  private SinkCounter sinkCounter;
  private int batchCounter;
  private long eventCounter;
  private long processSize;

  protected boolean closed; // flag indicating HiveWriter was closed
  private boolean autoCreatePartitions;

  private boolean hearbeatNeeded = false;

  HiveWriter(HiveEndPoint endPoint, int txnsPerBatch,
             boolean autoCreatePartitions, long callTimeout,
             ExecutorService callTimeoutPool, String hiveUser,
             HiveEventSerializer serializer, SinkCounter sinkCounter)
          throws IOException, ClassNotFoundException, InterruptedException
                 , StreamingException {
    this.autoCreatePartitions = autoCreatePartitions;
    this.sinkCounter = sinkCounter;
    this.callTimeout = callTimeout;
    this.callTimeoutPool = callTimeoutPool;
    this.endPoint = endPoint;
    this.connection = newConnection(hiveUser);
    this.txnsPerBatch = txnsPerBatch;
    this.serializer = serializer;
    this.recordWriter = serializer.createRecordWriter(endPoint);
    this.txnBatch = nextTxnBatch(recordWriter);
    this.closed = false;
    this.lastUsed = System.currentTimeMillis();
  }

  @Override
  public String toString() {
    return endPoint.toString();
  }

  /**
   * Clear the class counters
   */
  private void resetCounters() {
    eventCounter = 0;
    processSize = 0;
    batchCounter = 0;
  }

  void setHearbeatNeeded() {
    hearbeatNeeded = true;
  }



  /**
   * Write data, update stats <br />
   *
   * @throws IOException
   * @throws InterruptedException
   */
  public synchronized void write(final Event event)
          throws IOException, InterruptedException {
    checkAndThrowInterruptedException();
    // If idleFuture is not null, cancel it before we move forward to avoid a
    // close call in the middle of the append.
    if(idleFuture != null) {
      idleFuture.cancel(false);

      if(!idleFuture.isDone()) {
        try {
          idleFuture.get(callTimeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException ex) {
          LOG.warn("Timeout while trying to cancel closing of idle file. Idle" +
            " file close may have failed", ex);
        } catch (Exception ex) {
          LOG.warn("Error while trying to cancel closing of idle file. ", ex);
        }
      }
      idleFuture = null;
    }

    if (closed) {
      throw new IllegalStateException("This hive streaming writer was closed " +
        "and thus no longer able to write : " + endPoint);
    }

    // write the event
    try {
      sinkCounter.incrementEventDrainAttemptCount();
      LOG.debug("Writing event to {}", endPoint);
      callWithTimeout(new CallRunner<Void>() {
        @Override
        public Void call() throws Exception {
          serializer.write(txnBatch, event);
          return null;
        }
      });
    } catch (Exception e) {
      LOG.warn("Failed writing to EndPoint : " + endPoint
              + ". TxnID : " + txnBatch.getCurrentTxnId()
              + ". Closing Transaction Batch and rethrowing exception.");
      throw new IOException("Write to hive endpoint failed: " + endPoint, e);
    }

    // Update Statistics
    processSize += event.getBody().length;
    eventCounter++;
  }

  /**
   * Commits the current Txn.
   * If 'rollToNext' is true, will switch to next Txn in batch or to a
   *       new TxnBatch if current Txn batch is exhausted
   * TODO: see what to do when there are errors in each IO call stage
   */
  public void flush(boolean rollToNext)
          throws IOException, InterruptedException, StreamingException {
    if(hearbeatNeeded) {
      hearbeatNeeded = false;
      heartBeat();
    }
    lastUsed = System.currentTimeMillis();
    commitTxn();
    if(txnBatch.remainingTransactions() == 0) {
      closeTxnBatch();
      txnBatch = null;
      if(rollToNext) {
        txnBatch = nextTxnBatch(recordWriter);
      }
    }
    if(rollToNext) {
      LOG.debug("Switching to next Txn for {}", endPoint);
      txnBatch.beginNextTransaction(); // does not block
    }
  }

  /** Queues up a heartbeat request on the current and remaining txns using the
   *  heartbeatThdPool and returns immediately
   */
  public void heartBeat() throws InterruptedException {
    // 1) schedule the heartbeat on one thread in pool
    try {
      callWithTimeout(new CallRunner<Void>() {
        @Override
        public Void call() throws Exception {
          try {
            LOG.debug("Sending heartbeat on batch " + txnBatch);
            txnBatch.heartbeat();
          } catch (StreamingException e) {
            LOG.warn("Heartbeat error on batch " + txnBatch, e);
          }
          return null;
        }
      });
    } catch (IOException e) {
      LOG.warn("I/O error during heartbeat on batch " + txnBatch, e);
    }
  }

  /**
   * Close the Transaction Batch and connection
   * @throws IOException
   * @throws InterruptedException
   */
  public void close() throws IOException, InterruptedException {
    closeTxnBatch();
    closeConnection();
    closed = true;
    sinkCounter.incrementConnectionClosedCount();
  }

  private void closeConnection() throws IOException, InterruptedException {
    LOG.info("Closing connection to end point : {}", endPoint);
    callWithTimeout(new CallRunner<Void>() {
      @Override
      public Void call() throws Exception {
        connection.close(); // could block
        return null;
      }
    });
  }

  private void commitTxn() throws IOException, InterruptedException {
    LOG.debug("Committing Txn id {} to {}", txnBatch.getCurrentTxnId() , endPoint);
    callWithTimeout(new CallRunner<Void>() {
      @Override
      public Void call() throws Exception {
        txnBatch.commit(); // could block
        return null;
      }
    });
  }

  private StreamingConnection newConnection(final String proxyUser)
          throws IOException, InterruptedException {
    return  callWithTimeout(new CallRunner<StreamingConnection>() {
      @Override
      public StreamingConnection call() throws Exception {
        return endPoint.newConnection(autoCreatePartitions); // could block
      }
    });
  }

  private TransactionBatch nextTxnBatch(final RecordWriter recordWriter)
          throws IOException, InterruptedException, StreamingException {
    LOG.debug("Fetching new Txn Batch for {}", endPoint);
    TransactionBatch batch = callWithTimeout(new CallRunner<TransactionBatch>() {
              @Override
              public TransactionBatch call() throws Exception {
                return connection.fetchTransactionBatch(txnsPerBatch, recordWriter); // could block
              }
            });
    LOG.debug("Acquired {}. Switching to first txn", batch);
    batch.beginNextTransaction();
    return batch;
  }

  private void closeTxnBatch() throws IOException, InterruptedException {
    LOG.debug("Closing Txn Batch {}", txnBatch);
    callWithTimeout(new CallRunner<Void>() {
      @Override
      public Void call() throws Exception {
        txnBatch.close(); // could block
        return null;
      }
    });
  }

  /**
   * If the current thread has been interrupted, then throws an
   * exception.
   * @throws InterruptedException
   */
  private static void checkAndThrowInterruptedException()
          throws InterruptedException {
    if (Thread.currentThread().interrupted()) {
      throw new InterruptedException("Timed out before Hive call was made. "
              + "Your callTimeout might be set too low or Hive calls are "
              + "taking too long.");
    }
  }

  /**
   * Execute the callable on a separate thread and wait for the completion
   * for the specified amount of time in milliseconds. In case of timeout
   * cancel the callable and throw an IOException
   */
  private <T> T callWithTimeout(final CallRunner<T> callRunner)
    throws IOException, InterruptedException {
    Future<T> future = callTimeoutPool.submit(new Callable<T>() {
      @Override
      public T call() throws Exception {
//        return runPrivileged(new PrivilegedExceptionAction<T>() {
//          @Override
//          public T run() throws Exception {
            return callRunner.call();
//          }
//        });
      }
    });
    try {
      if (callTimeout > 0) {
        return future.get(callTimeout, TimeUnit.MILLISECONDS);
      } else {
        return future.get();
      }
    } catch (TimeoutException eT) {
      future.cancel(true);
      sinkCounter.incrementConnectionFailedCount();
      throw new IOException("Callable timed out after " + callTimeout + " ms" +
          " on EndPoint: " + endPoint,
        eT);
    } catch (ExecutionException e1) {
      sinkCounter.incrementConnectionFailedCount();
      Throwable cause = e1.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      } else if (cause instanceof InterruptedException) {
        throw (InterruptedException) cause;
      } else if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      } else if (cause instanceof Error) {
        throw (Error)cause;
      } else {
        throw new RuntimeException(e1);
      }
    } catch (CancellationException ce) {
      throw new InterruptedException(
        "Blocked callable interrupted by rotation event");
    } catch (InterruptedException ex) {
      LOG.warn("Unexpected Exception " + ex.getMessage(), ex);
      throw ex;
    }
  }

  long getLastUsed() {
    return lastUsed;
  }

  /**
   * Simple interface whose <tt>call</tt> method is called by
   * {#callWithTimeout} in a new thread inside a
   * {@linkplain java.security.PrivilegedExceptionAction#run()} call.
   * @param <T>
   */
  private interface CallRunner<T> {
    T call() throws Exception;
  }

}
