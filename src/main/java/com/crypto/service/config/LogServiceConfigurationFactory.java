package com.crypto.service.config;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Order;
import org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.filter.MarkerFilter;
import org.apache.logging.log4j.core.pattern.AnsiEscape;
import org.apache.logging.log4j.core.tools.picocli.CommandLine;

import java.net.URI;

@Plugin(name = "CustomConfigurationFactory", category = ConfigurationFactory.CATEGORY)
@Order(50)
public class LogServiceConfigurationFactory extends ConfigurationFactory {

  static Configuration createConfiguration(
      final String name, ConfigurationBuilder<BuiltConfiguration> builder) {
    builder
        .setConfigurationName(name)
        .setStatusLevel(Level.INFO)
        .add(
            builder
                .newFilter("ThresholdFilter", Filter.Result.ACCEPT, Filter.Result.NEUTRAL)
                .addAttribute("level", Level.DEBUG));

    AppenderComponentBuilder appenderBuilder =
        builder.newAppender("StdOut", ConsoleAppender.PLUGIN_NAME);
    appenderBuilder
        .addAttribute("target", ConsoleAppender.Target.SYSTEM_OUT)
        .add(
            builder
                .newLayout("PatternLayout")
                .addAttribute("pattern", "%highlight{%d [%t] %-5level: %msg%n%throwable}")
                .addAttribute("disableAnsi", false))
        .add(
            builder
                .newFilter("MarkerFilter", Filter.Result.DENY, Filter.Result.NEUTRAL)
                .addAttribute(MarkerFilter.ATTR_MARKER, "FLOW"));

    builder
        .add(appenderBuilder)
        .add(
            builder
                .newLogger("com.crypto.service", Level.INFO)
                .add(builder.newAppenderRef("StdOut"))
                .addAttribute("additivity", false))
        .add(builder.newRootLogger(Level.ERROR).add(builder.newAppenderRef("StdOut")));

    return builder.build();
  }

  @Override
  public Configuration getConfiguration(LoggerContext loggerContext, ConfigurationSource source) {
    return getConfiguration(loggerContext, source.toString(), null);
  }

  @Override
  public Configuration getConfiguration(
      LoggerContext loggerContext, String name, URI configLocation) {
    ConfigurationBuilder<BuiltConfiguration> builder = newConfigurationBuilder();
    return createConfiguration(name, builder);
  }

  @Override
  protected String[] getSupportedTypes() {
    return new String[] {"*"};
  }
}
