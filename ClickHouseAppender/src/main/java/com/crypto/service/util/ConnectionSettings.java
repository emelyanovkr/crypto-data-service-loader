package com.crypto.service.util;

import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.config.plugins.validation.constraints.Required;

import java.io.IOException;

@Plugin(
    name = "ConnectionSettings",
    category = Node.CATEGORY,
    elementType = "connectionSettings",
    printObject = true)
public class ConnectionSettings
{
  private static String host;
  private static int port;
  private static String database;
  private static String username;
  private static String password;
  private static String SSL;
  private static String socket_timeout;
  private static String max_execution_time;
  private static String custom_params;

  private ConnectionSettings(
      String host,
      int port,
      String database,
      String username,
      String password,
      String SSL,
      String socket_timeout,
      String max_execution_time,
      String custom_params) {
    ConnectionSettings.host = host;
    ConnectionSettings.port = port;
    ConnectionSettings.database = database;
    ConnectionSettings.username = username;
    ConnectionSettings.password = password;
    ConnectionSettings.SSL = SSL;
    ConnectionSettings.socket_timeout = socket_timeout;
    ConnectionSettings.max_execution_time = max_execution_time;
    ConnectionSettings.custom_params = custom_params;
  }

  public static ClickHouseNode initJavaClientConnection() throws IOException {
    return ClickHouseNode.builder()
        .host(host)
        .port(ClickHouseProtocol.HTTP, port)
        .database(database)
        .credentials(ClickHouseCredentials.fromUserAndPassword(username, password))
        .addOption(ClickHouseClientOption.SSL.getKey(), SSL)
        .addOption(ClickHouseClientOption.SOCKET_TIMEOUT.getKey(), socket_timeout)
        .addOption(ClickHouseClientOption.MAX_EXECUTION_TIME.getKey(), max_execution_time)
        .addOption(ClickHouseHttpOption.CUSTOM_PARAMS.getKey(), custom_params)
        .build();
  }

  @PluginFactory
  public static ConnectionSettings createConnectionHandler(
      @PluginAttribute("HOST") @Required(message = "No host provided") String host,
      @PluginAttribute("PORT") @Required(message = "No port provided") int port,
      @PluginAttribute("DATABASE") @Required(message = "No db provided") String database,
      @PluginAttribute("USERNAME") @Required(message = "No username provided") String username,
      @PluginAttribute("PASSWORD") @Required(message = "No password provided") String password,
      @PluginAttribute("SSL") String SSL,
      @PluginAttribute("SOCKET_TIMEOUT") String socket_timeout,
      @PluginAttribute("MAX_EXECUTION_TIME") String max_execution_time,
      @PluginAttribute("CUSTOM_PARAMS") String custom_params) {

    return new ConnectionSettings(
        host,
        port,
        database,
        username,
        password,
        SSL,
        socket_timeout,
        max_execution_time,
        custom_params);
  }
}
