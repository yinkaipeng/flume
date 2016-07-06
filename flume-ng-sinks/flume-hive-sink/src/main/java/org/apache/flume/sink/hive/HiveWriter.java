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
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.hcatalog.streaming.*;

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

  private long lastUsed; // time of last flush on this writer

  private SinkCounter sinkCounter;
  private int batchCounter;
  private long eventCounter;
  private long processSize;

  protected boolean closed; // flag indicating HiveWriter was closed
  private boolean autoCreatePartitions;

  private boolean hearbeatNeeded = false;
  private UserGroupInformation ugi;

  private final int writeBatchSz = 1000;
  private ArrayList<Event> batch = new ArrayList<Event>(writeBatchSz);

  HiveWriter(HiveEndPoint endPoint, int txnsPerBatch,
             boolean autoCreatePartitions, long callTimeout,
             ExecutorService callTimeoutPool, UserGroupInformation ugi,
             HiveEventSerializer serializer, SinkCounter sinkCounter)
          throws ConnectFailure, InterruptedException {
    try {
      this.autoCreatePartitions = autoCreatePartitions;
      this.sinkCounter = sinkCounter;
      this.callTimeout = callTimeout;
      this.callTimeoutPool = callTimeoutPool;
      this.endPoint = endPoint;
      this.ugi = ugi;
      this.connection = newConnection(ugi);
      this.txnsPerBatch = txnsPerBatch;
      this.serializer = serializer;
      this.recordWriter = serializer.createRecordWriter(endPoint);
      this.txnBatch = nextTxnBatch(recordWriter);
      this.txnBatch.beginNextTransaction();
      this.closed = false;
      this.lastUsed = System.currentTimeMillis();
    } catch (InterruptedException e) {
      throw e;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new ConnectFailure(endPoint, e);
    }
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

  public int getRemainingTxns() {
    return txnBatch.remainingTransactions();
  }


  /**
   * Write data, update stats
   * @param event
   * @throws WriteFailure - other streaming io error
   * @throws InterruptedException
   */
  public synchronized void write(final Event event)
          throws WriteFailure, InterruptedException {
    if (closed) {
      throw new IllegalStateException("Writer closed. Cannot write to : " + endPoint);
    }

    batch.add(event);
    if(batch.size()== writeBatchSz) {
      // write the event
      writeEventBatchToSerializer();
    }

    // Update Statistics
    processSize += event.getBody().length;
    eventCounter++;
  }

  private void writeEventBatchToSerializer()
          throws InterruptedException, WriteFailure {
    try {
      timedCall(new CallRunner1<Void>() {
        @Override
        public Void call() throws InterruptedException, StreamingException {
          try {
            for (Event event : batch) {
              try {
                serializer.write(txnBatch, event);
              } catch (SerializationError err) {
                LOG.info("Parse failed : {}  : {}", err.getMessage(), new String(event.getBody()));
              }
            }
            return null;
          } catch (IOException e) {
            throw new StreamingIOFailure(e.getMessage(), e);
          }
        }
      });
      batch.clear();
    } catch (StreamingException e) {
      throw new WriteFailure(endPoint, txnBatch.getCurrentTxnId(), e);
    } catch (TimeoutException e) {
      throw new WriteFailure(endPoint, txnBatch.getCurrentTxnId(), e);
    }
  }

  /**
   * Commits the current Txn.
   * If 'rollToNext' is true, will switch to next Txn in batch or to a
   *       new TxnBatch if current Txn batch is exhausted
   */
  public void flush(boolean rollToNext)
          throws CommitFailure, TxnBatchFailure, TxnFailure, InterruptedException,
          WriteFailure {
    if(!batch.isEmpty()) {
      writeEventBatchToSerializer();
      batch.clear();
    }
    //0 Heart beat on TxnBatch
    if(hearbeatNeeded) {
      hearbeatNeeded = false;
      heartBeat();
    }
    lastUsed = System.currentTimeMillis();

    try {
      //1 commit txn & close batch if needed
      commitTxn();
      if(txnBatch.remainingTransactions() == 0) {
        closeTxnBatch();
        txnBatch = null;
        if(rollToNext) {
          txnBatch = nextTxnBatch(recordWriter);
        }
      }

      //2 roll to next Txn
      if(rollToNext) {
        LOG.debug("Switching to next Txn for {}", endPoint);
        txnBatch.beginNextTransaction(); // does not block
      }
    } catch (StreamingException e) {
      throw new TxnFailure(txnBatch, e);
    }
  }

  /**
   * Aborts the current Txn
   * @throws InterruptedException
   */
  public void abort()  throws InterruptedException {
    batch.clear();
    abortTxn();
  }

  /** Queues up a heartbeat request on the current and remaining txns using the
   *  heartbeatThdPool and returns immediately
   */
  public void heartBeat() throws InterruptedException  {
    // 1) schedule the heartbeat on one thread in pool
    try {
      timedCall(new CallRunner1<Void>() {
        @Override
        public Void call() throws StreamingException {
          LOG.info("Sending heartbeat on batch " + txnBatch);
          txnBatch.heartbeat();
          return null;
        }
      });
    } catch (InterruptedException e) {
      throw e;
    } catch (Exception e) {
      LOG.warn("Unable to send heartbeat on Txn Batch " + txnBatch, e);
      // Suppressing exceptions as we don't care for errors on heartbeats
    }
  }

  /**
   * Close the Transaction Batch and connection
   * @throws IOException
   * @throws InterruptedException
   */
  public void close() throws InterruptedException {
    batch.clear();
    abortRemainingTxns();
    closeTxnBatch();
    closeConnection();
    closed = true;
  }


  private void abortRemainingTxns() throws InterruptedException {
      try {
        if ( !isClosed(txnBatch.getCurrentTransactionState()) ) {
          abortCurrTxnHelper();
        }

        // recursively abort remaining txns
        if(txnBatch.remainingTransactions()>0) {
          timedCall(
                  new CallRunner1<Void>() {
                    @Override
                    public Void call() throws StreamingException, InterruptedException {
                      txnBatch.beginNextTransaction();
                      return null;
                    }
                  });
          abortRemainingTxns();
        }
      } catch (StreamingException e) {
        LOG.warn("Error when aborting remaining transactions in batch " + txnBatch, e);
        return;
      } catch (TimeoutException e) {
        LOG.warn("Timed out when aborting remaining transactions in batch " + txnBatch, e);
        return;
      }
  }

  private void abortCurrTxnHelper() throws TimeoutException, InterruptedException {
    try {
      timedCall(
              new CallRunner1<Void>() {
                @Override
                public Void call() throws StreamingException, InterruptedException {
                  txnBatch.abort();
                  LOG.info("Aborted txn " + txnBatch.getCurrentTxnId());
                  return null;
                }
              }
      );
    } catch (StreamingException e) {
      LOG.warn("Unable to abort transaction " + txnBatch.getCurrentTxnId(), e);
      // continue to attempt to abort other txns in the batch
    }
  }

  private boolean isClosed(TransactionBatch.TxnState txnState) {
    if(txnState == TransactionBatch.TxnState.COMMITTED)
      return true;
    if(txnState == TransactionBatch.TxnState.ABORTED)
      return true;
    return false;
  }

  public void closeConnection() throws InterruptedException {
    LOG.debug("Closing connection to EndPoint : {}", endPoint);
    try {
      timedCall(new CallRunner1<Void>() {
        @Override
        public Void call() {
          connection.close(); // could block
          return null;
        }
      });
      sinkCounter.incrementConnectionClosedCount();
    } catch (Exception e) {
      LOG.warn("Error closing connection to EndPoint : " + endPoint, e);
      // Suppressing exceptions as we don't care for errors on connection close
    }
  }

  private void commitTxn() throws CommitFailure, InterruptedException {
    LOG.info("Committing Txn id {} to {}", txnBatch.getCurrentTxnId(), endPoint);
    try {
      timedCall(new CallRunner1<Void>() {
        @Override
        public Void call() throws StreamingException, InterruptedException {
          txnBatch.commit(); // could block
          return null;
        }
      });
    } catch (Exception e) {
      throw new CommitFailure(endPoint, txnBatch.getCurrentTxnId(), e);
    }
  }

  private void abortTxn() throws InterruptedException {
    LOG.info("Aborting Txn id {} on End Point {}", txnBatch.getCurrentTxnId(), endPoint);
    try {
      timedCall(new CallRunner1<Void>() {
        @Override
        public Void call() throws StreamingException, InterruptedException {
          txnBatch.abort(); // could block
          return null;
        }
      });
    } catch (InterruptedException e) {
      throw e;
    } catch (TimeoutException e) {
      LOG.warn("Timeout while aborting Txn " + txnBatch.getCurrentTxnId() + " on EndPoint: " + endPoint, e);
    } catch (Exception e) {
      LOG.warn("Error aborting Txn " + txnBatch.getCurrentTxnId() + " on EndPoint: " + endPoint, e);
      // Suppressing exceptions as we don't care for errors on abort
    }
  }

  private StreamingConnection newConnection(final UserGroupInformation ugi)
          throws InterruptedException, ConnectFailure {
    try {
      return  timedCall(new CallRunner1<StreamingConnection>() {
        @Override
        public StreamingConnection call() throws InterruptedException, StreamingException {
          return endPoint.newConnection(autoCreatePartitions, null, ugi); // could block
        }
      });
    } catch (Exception e) {
      throw new ConnectFailure(endPoint, e);
    }
  }

  private TransactionBatch nextTxnBatch(final RecordWriter recordWriter)
          throws InterruptedException, TxnBatchFailure {
    LOG.debug("Fetching new Txn Batch for {}", endPoint);
    TransactionBatch batch = null;
    try {
      batch = timedCall(new CallRunner1<TransactionBatch>() {
        @Override
        public TransactionBatch call() throws InterruptedException, StreamingException {
          return connection.fetchTransactionBatch(txnsPerBatch, recordWriter); // could block
        }
      });
      LOG.info("Acquired Transaction batch {}", batch);
    } catch (Exception e) {
      throw new TxnBatchFailure(endPoint, e);
    }
    return batch;
  }

  private void closeTxnBatch() throws InterruptedException {
    try {
      LOG.info("Closing Txn Batch {}.", txnBatch);
      timedCall(new CallRunner1<Void>() {
        @Override
        public Void call() throws InterruptedException, StreamingException {
          txnBatch.close(); // could block
          return null;
        }
      });
    } catch (InterruptedException e) {
      throw e;
    } catch (Exception e) {
      LOG.warn("Error closing Txn Batch " + txnBatch, e);
      // Suppressing exceptions as we don't care for errors on batch close
    }
  }

  private <T> T timedCall(final CallRunner1<T> callRunner)
          throws TimeoutException, InterruptedException, StreamingException {
    Future<T> future = callTimeoutPool.submit(new Callable<T>() {
      @Override
      public T call() throws StreamingException, InterruptedException, Failure {
        return callRunner.call();
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
      throw eT;
    } catch (ExecutionException e1) {
      sinkCounter.incrementConnectionFailedCount();
      Throwable cause = e1.getCause();
      if (cause instanceof IOException ) {
        throw new StreamingIOFailure("I/O Failure", (IOException) cause);
      } else if (cause instanceof StreamingException) {
        throw (StreamingException) cause;
      } else if (cause instanceof TimeoutException) {
        throw new StreamingException("Operation Timed Out.", (TimeoutException) cause);
      } else if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      } else if (cause instanceof InterruptedException) {
        throw (InterruptedException) cause;
      }
      throw new RuntimeException(e1);
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


  private interface CallRunner1<T> {
    T call() throws StreamingException, InterruptedException, Failure;
  }


  public static class Failure extends Exception {
    public Failure(String msg, Throwable cause) {
      super(msg, cause);
    }
  }

  public static class WriteFailure extends Failure {
    public WriteFailure(HiveEndPoint endPoint, Long currentTxnId, Throwable cause) {
      super("Failed writing to : " + endPoint + ". TxnID : " + currentTxnId, cause);
    }
  }

  public static class CommitFailure extends Failure {
    public CommitFailure(HiveEndPoint endPoint, Long txnID, Throwable cause) {
      super("Commit of Txn " + txnID +  " failed on EndPoint: " + endPoint, cause);
    }
  }

  public static class ConnectFailure extends Failure {
    public ConnectFailure(HiveEndPoint ep, Throwable cause) {
      super("Failed connecting to EndPoint " + ep, cause);
    }
  }

  public static class TxnBatchFailure extends Failure {
    public TxnBatchFailure(HiveEndPoint ep, Throwable cause) {
      super("Failed acquiring Transaction Batch from EndPoint: " + ep, cause);
    }
  }

  private class TxnFailure extends Failure {
    public TxnFailure(TransactionBatch txnBatch, Throwable cause) {
      super("Failed switching to next Txn in TxnBatch " + txnBatch, cause);
    }
  }
}
