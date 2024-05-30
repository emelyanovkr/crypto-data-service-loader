package com.crypto.service.util;

import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import java.util.Properties;

public class ConnectionHandler {

  public static ClickHouseNode initClickHouseConnection() {
    Properties properties = PropertiesLoader.loadProjectConfig();
    return initClickHouseConnection(properties);
  }

  public static ClickHouseNode initClickHouseConnection(Properties properties) {
    String host = properties.getProperty("HOST");
    int port = Integer.parseInt(properties.getProperty("PORT"));
    String database = properties.getProperty("DATABASE");
    String username = properties.getProperty("USERNAME");
    String password = properties.getProperty("PASSWORD");
    String ssl = properties.getProperty("SSL");
    String customParams = properties.getProperty(ClickHouseHttpOption.CUSTOM_PARAMS.getKey());
    String socketTimeout = properties.getProperty(ClickHouseClientOption.SOCKET_TIMEOUT.getKey());
    String socketKeepAlive =
        properties.getProperty(ClickHouseClientOption.SOCKET_KEEPALIVE.getKey());
    String connectionTimeout =
        properties.getProperty(ClickHouseClientOption.CONNECTION_TIMEOUT.getKey());

    return initClickHouseConnection(
        host,
        port,
        database,
        username,
        password,
        ssl,
        customParams,
        socketTimeout,
        socketKeepAlive,
        connectionTimeout);
  }

  public static ClickHouseNode initClickHouseConnection(
      String host,
      int port,
      String database,
      String username,
      String password,
      String ssl,
      String customParams,
      String socketTimeout,
      String socketKeepAlive,
      String connectionTimeout) {
    return ClickHouseNode.builder()
        .host(host)
        .port(ClickHouseProtocol.HTTP, port)
        .database(database)
        .credentials(ClickHouseCredentials.fromUserAndPassword(username, password))
        .addOption(ClickHouseClientOption.SSL.getKey(), ssl)
        .addOption(ClickHouseHttpOption.CUSTOM_PARAMS.getKey(), customParams)
        .addOption(ClickHouseClientOption.SOCKET_TIMEOUT.getKey(), socketTimeout)
        .addOption(ClickHouseClientOption.SOCKET_KEEPALIVE.getKey(), socketKeepAlive)
        .addOption(ClickHouseClientOption.CONNECTION_TIMEOUT.getKey(), connectionTimeout)
        .build();
  }
}
