package com.crypto.service.util;

import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.crypto.service.MainApplication;
import com.crypto.service.config.DatabaseConfig;

public class ConnectionHandler {

  public static ClickHouseNode initClickHouseConnection() {
    DatabaseConfig databaseConfig = MainApplication.applicationConfig.getDatabaseConfig();
    return initClickHouseConnection(databaseConfig);
  }

  public static ClickHouseNode initClickHouseConnection(DatabaseConfig databaseConfig) {
    String host = databaseConfig.getHost();
    int port = databaseConfig.getPort();
    String database = databaseConfig.getDatabase();
    String username = databaseConfig.getUsername();
    String password = databaseConfig.getPassword();
    String ssl = databaseConfig.getSsl();
    String customParams = databaseConfig.getCustomHttpParams();
    String socketTimeout = databaseConfig.getSocketTimeout();
    String socketKeepAlive = databaseConfig.getSocketKeepAlive();
    String connectionTimeout = databaseConfig.getConnectTimeout();

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
