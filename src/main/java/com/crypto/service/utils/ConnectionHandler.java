package com.crypto.service.utils;

import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseNodes;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;
import reactor.core.publisher.Mono;

import java.sql.SQLException;
import java.util.Properties;

public class ConnectionHandler {

  public static Mono<Connection> initR2DBCConnection() {
    Properties properties = PropertiesLoader.loadR2DBCProp();

    ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, properties.getProperty("DRIVER"))
            .option(ConnectionFactoryOptions.PROTOCOL, properties.getProperty("PROTOCOL"))
            .option(ConnectionFactoryOptions.HOST, properties.getProperty("HOST"))
            .option(ConnectionFactoryOptions.PORT, Integer.parseInt(properties.getProperty("PORT")))
            .option(ConnectionFactoryOptions.USER, properties.getProperty("USER"))
            .option(ConnectionFactoryOptions.PASSWORD, properties.getProperty("PASSWORD"))
            .option(ConnectionFactoryOptions.DATABASE, properties.getProperty("DATABASE"))
            .option(
                ConnectionFactoryOptions.SSL, Boolean.parseBoolean(properties.getProperty("SSL")))
            .build();

    return Mono.from(ConnectionFactories.get(options).create());
  }

  private static String getURL() {
    Properties properties = PropertiesLoader.loadJDBCProp();
    return "jdbc:ch:https://"
        + properties.getProperty("HOST")
        + ":"
        + properties.getProperty("PORT")
        + "/"
        + properties.getProperty("DATABASE")
        + "?ssl="
        + properties.getProperty("SSL");
  }

  public static ClickHouseConnection initJDBCConnection() throws SQLException {
    Properties properties = PropertiesLoader.loadJDBCProp();

    ClickHouseDataSource dataSource = new ClickHouseDataSource(getURL(), properties);

    return dataSource.getConnection(
        properties.getProperty("USERNAME"), properties.getProperty("PASSWORD"));
  }

  public static ClickHouseNode initJavaClientConnection() {
    Properties properties = PropertiesLoader.loadJDBCProp();

    return ClickHouseNode.builder()
      .host(properties.getProperty("HOST"))
      .port(ClickHouseProtocol.HTTP, Integer.valueOf(properties.getProperty("PORT")))
      .database(properties.getProperty("DATABASE"))
      .credentials(ClickHouseCredentials.fromUserAndPassword(properties.getProperty("USERNAME"), properties.getProperty("PASSWORD")))
      .addOption(ClickHouseClientOption.SSL.getKey(), properties.getProperty("SSL"))
      .build();
  }
}
