package com.crypto.service.util;

import com.google.common.io.Resources;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesLoader {
  private static String RESOURCE_NAME = "config.properties";

  public static Properties loadProjectConfig() {
    Properties properties = new Properties();
    try (InputStream input = Resources.getResource(RESOURCE_NAME).openStream()) {
      properties.load(input);
    } catch (IOException | IllegalArgumentException e) {
      System.err.println(PropertiesLoader.class.getName() + " FAILED TO LOAD CONFIGURATION " + e.getMessage());
      throw new RuntimeException(e);
    }
    return properties;
  }
}
