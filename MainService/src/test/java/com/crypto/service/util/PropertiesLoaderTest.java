package com.crypto.service.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PropertiesLoaderTest
{
  Properties properties;

  @BeforeEach
  public void setProperties() {
    InitData.setPropertiesField("test.properties");
    properties = PropertiesLoader.loadProjectConfig();
  }

  @Test
  public void testLoadedFields() {
    assertEquals("SOME_VALUE", properties.getProperty("FIELD_ONE"));
    assertEquals("VALUE_TWO", properties.getProperty("FIELD_TWO"));
    assertEquals("new_host.ws3amazon.com.com", properties.getProperty("HOST_EXAMPLE"));
  }

  @Test
  public void testThrownException() {
    InitData.setPropertiesField("no_property");
    assertThrows(RuntimeException.class, PropertiesLoader::loadProjectConfig);
  }
}
