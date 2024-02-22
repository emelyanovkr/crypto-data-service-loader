package com.crypto.service.utils;

import com.google.common.io.Resources;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesLoader {
  private static String getResource(final String resourceName) throws IOException {
    return Resources.getResource(resourceName).getPath();
  }

  public static Properties loadJDBCProp() {
    Properties properties = new Properties();
    try (InputStream input = new FileInputStream(getResource("jdbc_connection.properties"))) {
      properties.load(input);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return properties;
  }

  public static Properties loadR2DBCProp() {
    Properties properties = new Properties();
    try (InputStream input = new FileInputStream(getResource("r2dbc_connection.properties"))) {
      properties.load(input);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return properties;
  }
}
