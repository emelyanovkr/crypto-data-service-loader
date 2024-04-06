package com.crypto.service.appender;

import com.crypto.service.dao.ClickHouseDAO;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

@Plugin(
    name = "ClickHouseAppender",
    category = Core.CATEGORY_NAME,
    elementType = Appender.ELEMENT_TYPE,
    printObject = true)
public class ClickHouseAppender extends AbstractAppender {
  private final ClickHouseDAO clickHouseDAO;

  private ClickHouseAppender(
      String name,
      Filter filter,
      Layout<String> layout,
      boolean ignoreExceptions,
      Property[] properties) {
    super(name, filter, layout, false, properties);
    clickHouseDAO = ClickHouseDAO.getInstance();
  }

  @PluginFactory
  public static ClickHouseAppender createAppender(
      @PluginAttribute("name") String name,
      @PluginElement("Filters") Filter filter,
      @PluginElement("layout") Layout<String> layout,
      @PluginAttribute("ignoreExceptions") boolean ignoreExceptions) {
    if (name == null) {
      LOGGER.info("No name provided for ClickHouseAppender, default name is set");
      name = "ClickHouseAppender";
    }

    if (layout == null) {
      LOGGER.error("No layout provided for ClickHouseAppender, exit...");
      throw new RuntimeException();
    }

    return new ClickHouseAppender(name, filter, layout, ignoreExceptions, null);
  }

  @Override
  public void append(LogEvent event) {
    String serializedEvent = (String) getLayout().toSerializable(event);

    System.out.print(serializedEvent);
    clickHouseDAO.insertString(
        (event.getTimeMillis() + "\t" + serializedEvent)
            .replaceAll("\\r\\n", "\n")
            .replaceAll("\"timestamp\" : \"\"", ""));
  }
}
