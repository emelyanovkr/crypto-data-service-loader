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

  public static ClickHouseNode initJavaClientConnection() throws IOException
  {
    Properties properties = PropertiesLoader.loadProjectConfig();

    return ClickHouseNode.builder()
        .host(properties.getProperty("HOST"))
        .port(ClickHouseProtocol.HTTP, Integer.valueOf(properties.getProperty("PORT")))
        .database(properties.getProperty("DATABASE"))
        .credentials(
            ClickHouseCredentials.fromUserAndPassword(
                properties.getProperty("USERNAME"), properties.getProperty("PASSWORD")))
        .addOption(ClickHouseClientOption.SSL.getKey(), properties.getProperty("SSL"))
        .addOption(
            ClickHouseHttpOption.CUSTOM_PARAMS.getKey(),
            properties.getProperty(ClickHouseHttpOption.CUSTOM_PARAMS.getKey()))
        .addOption(
            ClickHouseClientOption.SOCKET_TIMEOUT.getKey(),
            properties.getProperty(ClickHouseClientOption.SOCKET_TIMEOUT.getKey()))
        .addOption(
            ClickHouseClientOption.MAX_EXECUTION_TIME.getKey(),
            properties.getProperty(ClickHouseClientOption.MAX_EXECUTION_TIME.getKey()))
        .build();
  }

  @Deprecated
  public static ClickHouseConnection initJDBCConnection() throws SQLException, IOException
  {
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

    properties.setProperty("localFile", "true");
    ClickHouseDataSource dataSource = new ClickHouseDataSource(url_connection, properties);
    ClickHouseConnection connection =
        dataSource.getConnection(
            properties.getProperty("USERNAME"), properties.getProperty("PASSWORD"));

    return connection;
  }
}
