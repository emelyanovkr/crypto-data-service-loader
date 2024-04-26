package com.crypto.service.util;

import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.crypto.service.dao.ClickHouseDAO;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionHandler {

  private static AtomicInteger enterCounter = new AtomicInteger(0);

  public static ClickHouseNode initClickHouseConnection() {
    Properties properties = PropertiesLoader.loadProjectConfig();
    return initClickHouseConnection(properties);
  }

  public static ClickHouseNode initClickHouseConnection(Properties properties) {
    String host = properties.getProperty("HOST");
    int port = Integer.valueOf(properties.getProperty("PORT"));
    String database = properties.getProperty("DATABASE");
    String username = properties.getProperty("USERNAME");
    String password = properties.getProperty("PASSWORD");
    String ssl = properties.getProperty("SSL");
    String customParams = properties.getProperty(ClickHouseHttpOption.CUSTOM_PARAMS.getKey());
    String socketTimeout = properties.getProperty(ClickHouseClientOption.SOCKET_TIMEOUT.getKey());
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
      String connectionTimeout) {
    return ClickHouseNode.builder()
        .host(host)
        .port(ClickHouseProtocol.HTTP, port)
        .database(database)
        .credentials(ClickHouseCredentials.fromUserAndPassword(username, password))
        .addOption(ClickHouseClientOption.SSL.getKey(), ssl)
        .addOption(ClickHouseHttpOption.CUSTOM_PARAMS.getKey(), customParams)
        .addOption(ClickHouseClientOption.SOCKET_TIMEOUT.getKey(), socketTimeout)
        .addOption(ClickHouseClientOption.CONNECTION_TIMEOUT.getKey(), connectionTimeout)
        .build();
  }

  public static ClickHouseDAO reconnect() {
    if (enterCounter.get() == 0) {
      try {
        enterCounter.incrementAndGet();
        return new ClickHouseDAO();
      } finally {
        enterCounter.set(0);
      }
    }
    return null;
  }

  public static String testConnection() {
    Properties properties = PropertiesLoader.loadProjectConfig();
    String curlQuery =
        "curl --user "
            + properties.getProperty("USERNAME")
            + ":"
            + properties.getProperty("PASSWORD")
            + " https://"
            + properties.getProperty("HOST")
            + ":"
            + properties.getProperty("PORT");

    try
    {
      Process process = Runtime.getRuntime().exec(curlQuery.split(" "));

      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      return reader.readLine();
    } catch (IOException e)
    {
      throw new RuntimeException(e);
    }
  }
}
