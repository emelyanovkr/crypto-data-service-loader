package com.crypto.service.appender;

import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.util.PropertiesLoader;

import java.io.IOException;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.ArrayBlockingQueue;

public class LogBuffer {

  private static LogBuffer instance;
  private final ClickHouseDAO clickHouseDAO;

  private final ArrayBlockingQueue<String> logBufferQueue;

  private final int BUFFER_SIZE;

  // seconds
  private final int TIMEOUT;
  private final Timer timer;

  private LogBuffer() {
    try {
      Properties bufferConfig = PropertiesLoader.loadProjectConfig();
      BUFFER_SIZE = Integer.parseInt(bufferConfig.getProperty("BUFFER_SIZE"));
      TIMEOUT = Integer.parseInt(bufferConfig.getProperty("BUFFER_TIMEOUT"));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    this.clickHouseDAO = ClickHouseDAO.getInstance();
    this.logBufferQueue = new ArrayBlockingQueue<>(BUFFER_SIZE);
    this.timer = new Timer();
    timer.cancel();
  }

  public static synchronized LogBuffer getInstance() {
    if (instance == null) {
      instance = new LogBuffer();
    }
    return instance;
  }

  private void insertLogMsg(String log) {
    if (logBufferQueue.remainingCapacity() == 0) {
      flushLogBuffer();
    }
    logBufferQueue.offer(log);
  }

  public void storeLogMsg(long timestamp, String log) {
    String tsvData = (timestamp + "\t" + log).replace("\\", "`");

    insertLogMsg(tsvData);
  }

  public void flushLogBuffer() {
    if (!logBufferQueue.isEmpty()) {
      clickHouseDAO.insertLogData(String.join("\n", logBufferQueue));
      logBufferQueue.clear();
    }
  }
}
