package com.clickhouse.appender.manager;

import com.clickhouse.appender.dao.ClickHouseLogDAO;
import com.clickhouse.appender.util.ConnectionSettings;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class LogBufferManager {
  protected ClickHouseLogDAO clickHouseLogDAO;
  final AtomicReference<LogBufferRecord> logBufferQueue;

  protected static class LogBufferRecord {
    final Queue<String> logBuffer;
    final AtomicInteger logBufferSize;
    final AtomicInteger referenceCounter;

    public LogBufferRecord() {
      this.logBuffer = new ConcurrentLinkedQueue<>();
      this.logBufferSize = new AtomicInteger(0);
      this.referenceCounter = new AtomicInteger(0);
    }
  }

  private final int bufferSize;
  private final int timeoutSec;
  private final int maxFlushAttempts;
  private final int sleepOnRetrySec;

  public LogBufferManager(
      int buffer_size,
      int timeoutSec,
      String tableName,
      int maxFlushAttempts,
      int sleepOnRetrySec,
      ConnectionSettings connectionSettings) {

    this.bufferSize = buffer_size;
    this.timeoutSec = timeoutSec;
    this.maxFlushAttempts = maxFlushAttempts;
    this.sleepOnRetrySec = sleepOnRetrySec;

    this.clickHouseLogDAO = new ClickHouseLogDAO(tableName, connectionSettings);
    this.logBufferQueue = new AtomicReference<>(new LogBufferRecord());

    Thread bufferService = new Thread(this::bufferManagement, "BUFFER-SERVICE-THREAD-1");
    bufferService.setDaemon(true);
    bufferService.start();

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread("SHUTDOWN-THREAD") {
              public void run() {
                flush();
              }
            });
  }

  protected boolean flushRequired(int logBufferQueueSize, long lastCallTime) {
    boolean timeoutElapsed = System.currentTimeMillis() - lastCallTime > timeoutSec * 1000L;
    boolean bufferSizeSufficient = logBufferQueueSize >= bufferSize;

    return bufferSizeSufficient || timeoutElapsed;
  }

  public void bufferManagement() {
    long lastCallTime = System.currentTimeMillis();
    while (true) {
      if (flushRequired(logBufferQueue.get().logBufferSize.get(), lastCallTime)) {
        lastCallTime = System.currentTimeMillis();

        flush();
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  protected void flush() {
    LogBufferRecord logBufferQueueToInsert = logBufferQueue.get();
    logBufferQueue.compareAndSet(logBufferQueueToInsert, new LogBufferRecord());

    // Spinning lock
    while (logBufferQueueToInsert.referenceCounter.get() != 0) {}

    boolean flushSuccessful = false;

    for (int i = 0; i < maxFlushAttempts; i++) {
      try {
        clickHouseLogDAO.insertLogData(String.join("\n", logBufferQueueToInsert.logBuffer));
        flushSuccessful = true;
        break;
      } catch (Exception e) {
        System.err.println(this.getClass().getName() + " EXCEPTION - " + e.getMessage());

        if (sleepOnRetrySec > 0) {
          try {
            Thread.sleep(sleepOnRetrySec);
          } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
          }
        }
      }
    }
    if (!flushSuccessful) {
      System.err.println(
          this.getClass().getName() + " LOST MESSAGES: " + logBufferQueueToInsert.logBuffer.size());
    }
  }

  protected String createLogMsg(long timestamp, String log) {
    return (timestamp + "\t" + log.replace("\t", "\\")).replace("\\", "`");
  }

  public void insertLogMsg(long timestamp, String log) {
    String tsvData = createLogMsg(timestamp, log);

    while (true) {
      LogBufferRecord logBufferRecord = logBufferQueue.get();

      try {
        getAndIncrement(logBufferRecord.referenceCounter);
        if (!logBufferQueue.compareAndSet(logBufferRecord, logBufferRecord)) {
          continue;
        }

        addToQueue(logBufferRecord.logBuffer, tsvData);
        logBufferRecord.logBufferSize.addAndGet(tsvData.getBytes().length);
        break;
      } finally {
        logBufferRecord.referenceCounter.getAndDecrement();
      }
    }
  }

  // for test purposes
  protected void getAndIncrement(AtomicInteger i) {
    i.incrementAndGet();
  }

  protected <T> void addToQueue(Queue<T> queue, T element) {
    queue.add(element);
  }
}
