package com.clickhouse.appender;

import com.clickhouse.appender.manager.LogBufferManager;
import com.clickhouse.appender.util.ConnectionSettings;
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
  protected static final int DEFAULT_BUFFER_SIZE = 8192;
  protected static final int DEFAULT_FLUSH_TIMEOUT_SEC = 30;
  protected static final String DEFAULT_TABLE_NAME = "logs";
  protected static final int DEFAULT_MAX_FLUSH_ATTEMPTS = 3;
  protected static final int DEFAULT_SLEEP_ON_FLUSH_RETRY_SEC = 3;

  protected final LogBufferManager logBufferManager;

  private ClickHouseAppender(
      String name,
      Filter filter,
      Layout<String> layout,
      boolean ignoreExceptions,
      int bufferSize,
      int bufferFlushTimeoutSec,
      String tableName,
      int maxFlushAttempts,
      int sleepOnRetrySec,
      ConnectionSettings connectionSettings) {
    this(
        name,
        filter,
        layout,
        ignoreExceptions,
        new LogBufferManager(
            bufferSize,
            bufferFlushTimeoutSec,
            tableName,
            maxFlushAttempts,
            sleepOnRetrySec,
            connectionSettings));
  }

  public ClickHouseAppender(
      String name,
      Filter filter,
      Layout<String> layout,
      boolean ignoreExceptions,
      LogBufferManager logBufferManager) {
    super(name, filter, layout, ignoreExceptions, null);
    this.logBufferManager = logBufferManager;
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
      @PluginAttribute("maxFlushAttempts") int maxFlushAttempts,
      @PluginAttribute("sleepOnRetrySec") int sleepOnRetrySec,
      @PluginElement("ConnectionSettings") ConnectionSettings connectionSettings) {

    if (name == null) {
      LOGGER.info(
          "No name provided for ClickHouseAppender, default name is set - {}", DEFAULT_TABLE_NAME);
      name = DEFAULT_TABLE_NAME;
    }

    if (layout == null) {
      LOGGER.error("No layout provided for ClickHouseAppender, exit...");
      throw new RuntimeException();
    }

    if (bufferSize == 0) {
      LOGGER.info("No buffer size provided, default value is set - {}", DEFAULT_BUFFER_SIZE);
      bufferSize = DEFAULT_BUFFER_SIZE;
    }

    if (timeoutSec == 0) {
      LOGGER.info(
          "No timeout for flush provided, default value is set - {}", DEFAULT_FLUSH_TIMEOUT_SEC);
      timeoutSec = DEFAULT_FLUSH_TIMEOUT_SEC;
    }

    if (tableName == null) {
      LOGGER.info("No table provided, default table is set - {}", DEFAULT_TABLE_NAME);
      tableName = DEFAULT_TABLE_NAME;
    }

    if (maxFlushAttempts == 0) {
      LOGGER.info(
          "No max flush attempts provided, default value is set - {}", DEFAULT_MAX_FLUSH_ATTEMPTS);
      maxFlushAttempts = DEFAULT_MAX_FLUSH_ATTEMPTS;
    }

    if (sleepOnRetrySec == 0) {
      LOGGER.info(
          "No sleep retry count provided, default value is set - {}",
          DEFAULT_SLEEP_ON_FLUSH_RETRY_SEC);
      sleepOnRetrySec = DEFAULT_SLEEP_ON_FLUSH_RETRY_SEC;
    }

    return new ClickHouseAppender(
        name,
        filter,
        layout,
        ignoreExceptions,
        bufferSize,
        timeoutSec,
        tableName,
        maxFlushAttempts,
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
