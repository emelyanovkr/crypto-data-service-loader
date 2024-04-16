package com.crypto.service.appender;

import com.crypto.service.dao.ClickHouseLogDAO;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class LogBuffer {

  private static LogBuffer instance;
  private final ClickHouseLogDAO clickHouseLogDAO;

  private final AtomicReference<Pair<Queue<String>, AtomicInteger>> logBufferQueue;

  static class Pair<K, V> {
    final K first;
    final V second;

    Pair(K first, V second) {
      this.first = first;
      this.second = second;
    }

    public K getFirst() {
      return first;
    }

    public V getSecond() {
      return second;
    }
  }
  private static int BUFFER_SIZE;

  // seconds
  private static int TIMEOUT;
  private static String TABLE_NAME;

  private LogBuffer() {

    this.clickHouseLogDAO = new ClickHouseLogDAO(TABLE_NAME);
    this.logBufferQueue =
      new AtomicReference<>(new Pair<>(new ConcurrentLinkedQueue<>(), new AtomicInteger(0)));
  }

  public static synchronized LogBuffer getInstance() {
    if (instance == null) {
      instance = new LogBuffer();
    }
    return instance;
  }

  // TODO: How to pass parameters?
  public static synchronized void setParameters(int buffer_size, int timeout, String tableName) {
    TABLE_NAME = tableName;
    BUFFER_SIZE = buffer_size;
    TIMEOUT = timeout;

  }

  private boolean flushRequired(
      Pair<Queue<String>, AtomicInteger> logBufferQueue, long lastCallTime) {

    boolean timeoutElapsed = System.currentTimeMillis() - lastCallTime > TIMEOUT;
    boolean bufferSizeSufficient = logBufferQueue.second.get() >= BUFFER_SIZE;

    return bufferSizeSufficient || timeoutElapsed;
  }

  private void bufferManagement() {
    long lastCallTime = System.currentTimeMillis();
    while (true) {

      if (flushRequired(logBufferQueue.get(), lastCallTime)) {
        lastCallTime = System.currentTimeMillis();
        Pair<Queue<String>, AtomicInteger> logBufferQueueCopy = logBufferQueue.get();

        logBufferQueue.compareAndSet(
            logBufferQueueCopy, new Pair<>(new ConcurrentLinkedQueue<>(), new AtomicInteger(0)));

        clickHouseLogDAO.insertLogData(String.join("\n", logBufferQueueCopy.getFirst()));
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void insertLogMsg(long timestamp, String log) {
    String tsvData = (timestamp + "\t" + log).replace("\\", "`");

    Pair<Queue<String>, AtomicInteger> pair = logBufferQueue.get();
    pair.first.add(tsvData);
    pair.second.addAndGet(tsvData.getBytes().length);
  }
}
