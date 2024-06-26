package com.clickhouse.appender.dao;

import com.clickhouse.appender.util.ConnectionSettings;
import com.clickhouse.client.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ClickHouseLogDAO {

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
}
