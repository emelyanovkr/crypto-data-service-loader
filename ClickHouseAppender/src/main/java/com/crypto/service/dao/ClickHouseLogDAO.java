package com.crypto.service.dao;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseResponse;
import com.crypto.service.util.ConnectionSettings;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ClickHouseLogDAO {
  private final ClickHouseNode server;
  private final ClickHouseClient client;
  private final String tableName;

  public ClickHouseLogDAO(String tableName) {
    try {
      this.server = ConnectionSettings.initJavaClientConnection();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.client = ClickHouseClient.newInstance(server.getProtocol());
    this.tableName = tableName;
  }

  // TODO: provide to define database from connectionsettings?
  //   sqlInjection possible?
  public void insertLogData(String tsvData) {
    try (ClickHouseResponse response =
        client
            .write(server)
            .query("INSERT INTO tickets_data_db." + tableName)
            .data(new ByteArrayInputStream(tsvData.getBytes(StandardCharsets.UTF_8)))
            .executeAndWait()) {
    } catch (ClickHouseException e) {
      throw new RuntimeException("FAILED TO INSERT STRING - " + e.getMessage());
    }
  }
}
