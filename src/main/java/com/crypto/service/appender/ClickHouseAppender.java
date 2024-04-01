package com.crypto.service.appender;

import org.apache.logging.log4j.core.*;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

@Plugin(
    name = "ClickHouseAppender",
    category = Core.CATEGORY_NAME,
    elementType = Appender.ELEMENT_TYPE,
    printObject = true)
public class ClickHouseAppender extends AbstractAppender {
  private ClickHouseAppender(
      String name, Filter filter, Layout layout, boolean ignoreExceptions, Property[] properties) {
    super(name, filter, layout, false, properties);
  }

  @PluginFactory
  public static ClickHouseAppender createAppender(
    @PluginAttribute("name") String name,
    @PluginElement("Filters") Filter filter,
    @PluginElement("layout") Layout layout,
    @PluginAttribute("ignoreExceptions") boolean ignoreExceptions)
  {
    if (name == null)
    {
      LOGGER.info("No name provided for ClickHouseAppender, default name is set");
      name = "ClickHouseAppender";
    }

    if (layout == null)
    {
      layout = PatternLayout.createDefaultLayout();
    }

    return new ClickHouseAppender(name, filter, layout, ignoreExceptions, null);
  }

  @Override
  public void append(LogEvent event) {
    System.out.println("DEFAULT APPENDER: " + event.getMessage() + event.getLevel());
  }
}
