package com.crypto.service.util;

import com.google.common.io.Resources;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesLoader {
  private final static String RESOURCE_NAME = "config.properties";

  public static Properties loadProjectConfig() throws IOException
  {
    Properties properties = new Properties();
    try (InputStream input = Resources.getResource(RESOURCE_NAME).openStream()) {
      properties.load(input);
    }
    return properties;
  }
}
