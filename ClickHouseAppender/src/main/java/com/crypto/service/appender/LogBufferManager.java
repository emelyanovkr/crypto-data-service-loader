package com.crypto.service.appender;

import com.crypto.service.dao.ClickHouseLogDAO;
import com.crypto.service.util.ConnectionSettings;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class LogBufferManager {

  private ClickHouseLogDAO clickHouseLogDAO;

  private final AtomicReference<LogBufferRecord> logBufferQueue;

  static class LogBufferRecord {
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
  private final int flushRetryCount;
  private final ConnectionSettings connectionSettings;

  public LogBufferManager(
      int buffer_size,
      int timeoutSec,
      String tableName,
      int flushRetryCount,
      ConnectionSettings connectionSettings) {

    this.bufferSize = buffer_size;
    this.timeoutSec = timeoutSec;
    this.flushRetryCount = flushRetryCount;
    this.connectionSettings = connectionSettings;

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

  private boolean flushRequired(LogBufferRecord logBufferQueue, long lastCallTime) {

    boolean timeoutElapsed = System.currentTimeMillis() - lastCallTime > timeoutSec * 1000L;
    boolean bufferSizeSufficient = logBufferQueue.logBufferSize.get() >= bufferSize;

    return bufferSizeSufficient || timeoutElapsed;
  }

  public void bufferManagement() {
    long lastCallTime = System.currentTimeMillis();
    while (true) {

      if (flushRequired(logBufferQueue.get(), lastCallTime)) {
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

  private void flush() {
    LogBufferRecord logBufferQueueToInsert = logBufferQueue.get();
    logBufferQueue.compareAndSet(logBufferQueueToInsert, new LogBufferRecord());

    // Spinning lock
    while (logBufferQueueToInsert.referenceCounter.get() != 0) {}

    System.out.println("ENTERING FLUSH: " + Thread.currentThread().getName());
    for (int i = 0; i < flushRetryCount; i++) {
      try {
        clickHouseLogDAO.insertLogData(String.join("\n", logBufferQueueToInsert.logBuffer));
        break;
      } catch (Exception e) {
        // TODO: REMOVE DEBUG PRINT
        System.out.println("THREAD IS GOING TO SLEEP: " + Thread.currentThread().getName());

        try
        {
          Thread.sleep(10000);
        } catch (InterruptedException ex)
        {
          throw new RuntimeException(ex);
        }

        System.out.println(
            "THREAD IS WAKING UP, TRYING TO RECONNECT: " + Thread.currentThread().getName());
        this.clickHouseLogDAO =
            connectionSettings.retryConnection(clickHouseLogDAO, connectionSettings);
      }
    }
    clickHouseLogDAO.testQuery();
  }

  public void insertLogMsg(long timestamp, String log) {
    String tsvData = (timestamp + "\t" + log).replace("\\", "`");

    while (true) {
      LogBufferRecord LogBufferRecord = logBufferQueue.get();

      try {
        LogBufferRecord.referenceCounter.getAndIncrement();
        if (!logBufferQueue.compareAndSet(LogBufferRecord, LogBufferRecord)) {
          continue;
        }
        LogBufferRecord.logBuffer.add(tsvData);
        LogBufferRecord.logBufferSize.addAndGet(tsvData.getBytes().length);
        break;
      } finally {
        LogBufferRecord.referenceCounter.getAndDecrement();
      }
    }
  }
}
