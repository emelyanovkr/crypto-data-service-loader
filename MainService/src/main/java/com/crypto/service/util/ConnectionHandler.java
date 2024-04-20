package com.crypto.service.util;

import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionHandler {

  public static ClickHouseNode initClickHouseConnection() throws IOException {
    Properties properties = PropertiesLoader.loadProjectConfig();
    return initClickHouseConnection(properties);
  }

  public static ClickHouseNode initClickHouseConnection(Properties properties) throws IOException {
    String host = properties.getProperty("HOST");
    int port = Integer.valueOf(properties.getProperty("PORT"));
    String database = properties.getProperty("DATABASE");
    String username = properties.getProperty("USERNAME");
    String password = properties.getProperty("PASSWORD");
    String ssl = properties.getProperty("SSL");
    String customParams = properties.getProperty(ClickHouseHttpOption.CUSTOM_PARAMS.getKey());
    String socketTimeout = properties.getProperty(ClickHouseClientOption.SOCKET_TIMEOUT.getKey());
    String maxExecutionTime =
        properties.getProperty(ClickHouseClientOption.MAX_EXECUTION_TIME.getKey());

    return initClickHouseConnection(
        host,
        port,
        database,
        username,
        password,
        ssl,
        customParams,
        socketTimeout,
        maxExecutionTime);
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
      String maxExecutionTime)
      throws IOException {
    return ClickHouseNode.builder()
        .host(host)
        .port(ClickHouseProtocol.HTTP, port)
        .database(database)
        .credentials(ClickHouseCredentials.fromUserAndPassword(username, password))
        .addOption(ClickHouseClientOption.SSL.getKey(), ssl)
        .addOption(ClickHouseHttpOption.CUSTOM_PARAMS.getKey(), customParams)
        .addOption(ClickHouseClientOption.SOCKET_TIMEOUT.getKey(), socketTimeout)
        .addOption(ClickHouseClientOption.MAX_EXECUTION_TIME.getKey(), maxExecutionTime)
        .build();
  }

  @Deprecated
  public static ClickHouseConnection initJDBCConnection() throws SQLException, IOException {
    Properties properties = PropertiesLoader.loadProjectConfig();
    String url_connection =
        "jdbc:ch:https://"
            + properties.getProperty("HOST")
            + ":"
            + properties.getProperty("PORT")
            + "/"
            + properties.getProperty("DATABASE")
            + "?ssl="
            + properties.getProperty("SSL")
            + "&custom_http_params=async_insert=1,wait_for_async_insert=1";

    ClickHouseDataSource dataSource = new ClickHouseDataSource(url_connection, properties);
    return dataSource.getConnection(
        properties.getProperty("USERNAME"), properties.getProperty("PASSWORD"));
  }
}
