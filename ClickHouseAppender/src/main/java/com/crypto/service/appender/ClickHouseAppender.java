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

  private final LogBuffer logBuffer;

  private ClickHouseAppender(
      String name,
      Filter filter,
      Layout<String> layout,
      boolean ignoreExceptions,
      int bufferSize,
      int bufferFlushTimeout,
      String tableName,
      int flushRetryCount) {
    super(name, filter, layout, false, null);

    LogBuffer.setParameters(bufferSize, bufferFlushTimeout, tableName, flushRetryCount);
    this.logBuffer = LogBuffer.getInstance();

    // TODO: temporary to test logs sending
    Thread thread = new Thread(logBuffer::bufferManagement);
    thread.setDaemon(true);
    thread.start();
  }

  @PluginFactory
  public static ClickHouseAppender createAppender(
      @PluginAttribute("name") String name,
      @PluginElement("Filters") Filter filter,
      @PluginElement("layout") Layout<String> layout,
      @PluginAttribute("ignoreExceptions") boolean ignoreExceptions,
      @PluginAttribute("bufferSize") int bufferSize,
      @PluginAttribute("timeout") int timeout,
      @PluginAttribute("tableName") String tableName,
      @PluginAttribute("flushRetryCount") int flushRetryCount,
      // TODO remove in the future, implement ConnectionPool class
      @PluginElement("ConnectionSettings") ConnectionSettings settings) {

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

    if (timeout == 0) {
      LOGGER.info("No timeout provided, default value is set - 30");
      timeout = DEFAULT_TIMEOUT;
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
        name, filter, layout, ignoreExceptions, bufferSize, timeout, tableName, flushRetryCount);
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

    logBuffer.insertLogMsg(event.getTimeMillis(), serializedEvent);
  }
}
