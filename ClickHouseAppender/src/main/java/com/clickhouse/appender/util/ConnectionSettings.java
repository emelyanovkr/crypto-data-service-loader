package com.clickhouse.appender.util;

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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

@Plugin(
    name = "ConnectionSettings",
    category = Node.CATEGORY,
    elementType = "connectionSettings",
    printObject = true)
public class ConnectionSettings {
  public final String host;
  public final int port;
  public final String database;
  public final String username;
  public final String password;
  public final String SSL;
  public final String socketTimeout;
  public final String customParams;

  private ConnectionSettings(
      String host,
      int port,
      String database,
      String username,
      String password,
      String SSL,
      String socketTimeout,
      String customParams) {
    this.host = host;
    this.port = port;
    this.database = database;
    this.username = username;
    this.password = password;
    this.SSL = SSL;
    this.socketTimeout = socketTimeout;
    this.customParams = customParams;
  }

  public static ClickHouseNode initClickHouseConnection(ConnectionSettings connectionSettings) throws IOException {
      return initClickHouseConnection(
            connectionSettings.host,
            connectionSettings.port,
            connectionSettings.database,
            connectionSettings.username,
            connectionSettings.password,
            connectionSettings.SSL,
            connectionSettings.socketTimeout,
            connectionSettings.customParams);
  }

  public static ClickHouseNode initClickHouseConnection(
      String host,
      int port,
      String database,
      String username,
      String password,
      String SSL,
      String socketTimeout,
      String customParams)
      throws IOException {
    return ClickHouseNode.builder()
        .host(host)
        .port(ClickHouseProtocol.HTTP, port)
        .database(database)
        .credentials(ClickHouseCredentials.fromUserAndPassword(username, password))
        .addOption(ClickHouseClientOption.SSL.getKey(), SSL)
        .addOption(ClickHouseClientOption.SOCKET_TIMEOUT.getKey(), socketTimeout)
        .addOption(ClickHouseHttpOption.CUSTOM_PARAMS.getKey(), customParams)
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
      @PluginAttribute("SOCKET_TIMEOUT") String socketTimeout,
      @PluginAttribute("CUSTOM_PARAMS") String customParams) {

    return new ConnectionSettings(
        host,
        port,
        database,
        username,
        password,
        SSL,
        socketTimeout,
        customParams);
  }

  public String testConnection() {
    String curlQuery =
      "curl --user "
        + username
        + ":"
        + password
        + " https://"
        + host
        + ":"
        + port;

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
