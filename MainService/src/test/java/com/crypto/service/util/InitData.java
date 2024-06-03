package com.crypto.service.util;

import org.junit.platform.commons.util.ReflectionUtils;

import java.lang.reflect.Field;

public class InitData {
  public static void setPropertiesField(String propertiesName) {
    Field RESOURCE_NAME_TO_TEST_FIELD =
        ReflectionUtils.findFields(
                PropertiesLoader.class,
                f -> f.getName().equals("RESOURCE_NAME"),
                ReflectionUtils.HierarchyTraversalMode.TOP_DOWN)
            .get(0);

    RESOURCE_NAME_TO_TEST_FIELD.setAccessible(true);
    try {

      RESOURCE_NAME_TO_TEST_FIELD.set(PropertiesLoader.class, propertiesName);

    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
