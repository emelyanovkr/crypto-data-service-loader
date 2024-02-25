package com.crypto.service.utils;

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
            + properties.getProperty("SSL");

    System.out.println(url_connection);
    ClickHouseDataSource dataSource = new ClickHouseDataSource(url_connection);
    ClickHouseConnection connection =
        dataSource.getConnection(
            properties.getProperty("USERNAME"), properties.getProperty("PASSWORD"));
    connection.setAutoCommit(false);

    return connection;
  }
}
