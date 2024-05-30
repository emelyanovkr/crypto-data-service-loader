package com.crypto.service.util;

import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.config.ClickHouseClientOption;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConnectionHandlerTest {

  @BeforeEach
  public void setProperties() {
    InitData.setPropertiesField("config_test.properties");
  }

  @Test
  public void testLoadingSettingsInClickHouseNode() {
    ClickHouseNode server = ConnectionHandler.initClickHouseConnection();

    assertEquals("TEST_HOST", server.getHost());
    assertEquals(9400, server.getPort());
    assertEquals("TEST_DB_NAME", server.getDatabase().get());
    assertTrue(server.getConfig().isSsl());
    assertEquals(
        "async_insert=1, wait_for_async_insert=1", server.getOptions().get("custom_http_params"));
    assertEquals(300000, server.getConfig().getSocketTimeout());
    assertEquals(true, server.getConfig().getOption(ClickHouseClientOption.SOCKET_KEEPALIVE));
    assertEquals(5000, server.getConfig().getOption(ClickHouseClientOption.CONNECTION_TIMEOUT));
  }
}
