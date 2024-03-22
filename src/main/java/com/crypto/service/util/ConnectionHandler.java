package com.crypto.service.util;

import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.data.ClickHouseCompression;
import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;
import reactor.core.publisher.Mono;

import java.sql.SQLException;
import java.util.Properties;

public class ConnectionHandler {

  public static ClickHouseConnection initJDBCConnection() throws SQLException {
    Properties properties = PropertiesLoader.loadJDBCProp();
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

  public static ClickHouseNode initJavaClientConnection() {
    Properties properties = PropertiesLoader.loadJDBCProp();

    return ClickHouseNode.builder()
        .host(properties.getProperty("HOST"))
        .port(ClickHouseProtocol.HTTP, Integer.valueOf(properties.getProperty("PORT")))
        .database(properties.getProperty("DATABASE"))
        .credentials(
            ClickHouseCredentials.fromUserAndPassword(
                properties.getProperty("USERNAME"), properties.getProperty("PASSWORD")))
        .addOption(ClickHouseClientOption.SSL.getKey(), properties.getProperty("SSL"))
        .addOption(
            ClickHouseHttpOption.CUSTOM_PARAMS.getKey(), "async_insert=1, wait_for_async_insert=1")
        .addOption(ClickHouseClientOption.SOCKET_TIMEOUT.getKey(), "300000")
        .addOption(ClickHouseClientOption.MAX_EXECUTION_TIME.getKey(), "300")
        .build();
  }
}
