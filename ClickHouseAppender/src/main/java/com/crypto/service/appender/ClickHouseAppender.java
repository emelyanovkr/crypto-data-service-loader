package com.crypto.service.appender;

import com.crypto.service.util.ConnectionSettings;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.*;

@Plugin(
    name = "ClickHouseAppender",
    category = Core.CATEGORY_NAME,
    elementType = Appender.ELEMENT_TYPE,
    printObject = true)
public class ClickHouseAppender extends AbstractAppender {

  private static final int DEFAULT_BUFFER_SIZE = 8192;
  private static final int DEFAULT_TIMEOUT = 30;
  private static final String DEFAULT_TABLE_NAME = "tickets_logs";

  private final LogBufferManager logBufferManager;

  private ClickHouseAppender(
      String name,
      Filter filter,
      Layout<String> layout,
      boolean ignoreExceptions,
      int bufferSize,
      int bufferFlushTimeoutSec,
      String tableName,
      int flushRetryCount,
      int sleepOnRetrySec,
      ConnectionSettings connectionSettings) {
    super(name, filter, layout, false, null);

    this.logBufferManager =
        new LogBufferManager(
            bufferSize,
            bufferFlushTimeoutSec,
            tableName,
            flushRetryCount,
            sleepOnRetrySec,
            connectionSettings);
  }

  @PluginFactory
  public static ClickHouseAppender createAppender(
      @PluginAttribute("name") String name,
      @PluginElement("Filters") Filter filter,
      @PluginElement("layout") Layout<String> layout,
      @PluginAttribute("ignoreExceptions") boolean ignoreExceptions,
      @PluginAttribute("bufferSize") int bufferSize,
      @PluginAttribute("timeoutSec") int timeoutSec,
      @PluginAttribute("tableName") String tableName,
      @PluginAttribute("flushRetryCount") int flushRetryCount,
      @PluginAttribute("sleepOnRetrySec") int sleepOnRetrySec,
      @PluginElement("ConnectionSettings") ConnectionSettings connectionSettings) {

    if (name == null) {
      LOGGER.info("No name provided for ClickHouseAppender, default name is set");
      name = "ClickHouseAppender";
    }

    if (layout == null) {
      LOGGER.error("No layout provided for ClickHouseAppender, exit...");
      throw new RuntimeException();
    }

    if (bufferSize == 0) {
      LOGGER.info("No buffer size provided, default value is set - 8192");
      bufferSize = DEFAULT_BUFFER_SIZE;
    }

    if (timeoutSec == 0) {
      LOGGER.info("No timeout provided, default value is set - 30");
      timeoutSec = DEFAULT_TIMEOUT;
    }

    if (tableName == null) {
      LOGGER.info("No table provided, default table is set - tickets_logs");
      tableName = DEFAULT_TABLE_NAME;
    }

    if (flushRetryCount == 0) {
      LOGGER.info("No flush retry count provided, default value is set - 3");
      flushRetryCount = 3;
    }

    return new ClickHouseAppender(
        name,
        filter,
        layout,
        ignoreExceptions,
        bufferSize,
        timeoutSec,
        tableName,
        flushRetryCount,
        sleepOnRetrySec,
        connectionSettings);
  }

  @Override
  public void append(LogEvent event) {
    String serializedEvent = (String) getLayout().toSerializable(event);

    // System delimiter is replaced with empty string to prevent
    // an error related to default delimiter:
    // \r\n causes ClickHouse to return an error
    if (serializedEvent.endsWith(System.lineSeparator())) {
      serializedEvent =
          serializedEvent.substring(0, serializedEvent.length() - System.lineSeparator().length());
    }

    logBufferManager.insertLogMsg(event.getTimeMillis(), serializedEvent);
  }
}
