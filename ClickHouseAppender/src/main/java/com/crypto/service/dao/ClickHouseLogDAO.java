package com.crypto.service.dao;

import com.clickhouse.client.*;
import com.crypto.service.util.ConnectionSettings;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ClickHouseLogDAO {

  // TODO: REFACTOR CONNECTION SETTINGS
  //  IMPLEMENT RECONNECT IN CLICKHOUSE DAO
  //  DIFFERENT APPENDERS
  //  CREATING TEST PLATFORM
  //  TEST DIFFERENT CONNECTION SITUATION ON SMALL THREAD COUNT

  private final ClickHouseNode server;
  private final ClickHouseClient client;
  private final String tableName;

  public ClickHouseLogDAO(String tableName, ConnectionSettings connectionSettings) {
    try {
      this.server = ConnectionSettings.initClickHouseConnection(connectionSettings);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.client = ClickHouseClient.newInstance(server.getProtocol());
    this.tableName = tableName;
  }

  public String getTableName() {
    return tableName;
  }

  public void insertLogData(String tsvData) throws ClickHouseException {
    try (ClickHouseResponse response =
        client
            .write(server)
            .query("INSERT INTO " + tableName)
            .data(new ByteArrayInputStream(tsvData.getBytes(StandardCharsets.UTF_8)))
            .executeAndWait()) {}
  }

  public void testQuery() {
    try (ClickHouseResponse response =
        client.read(server).query("SELECT COUNT(*) FROM " + tableName).executeAndWait()) {
      System.out.println("TOTAL COUNT: " + response.firstRecord().getValue(0).asLong());
    } catch (ClickHouseException e) {
      throw new RuntimeException(e);
    }
  }
}
