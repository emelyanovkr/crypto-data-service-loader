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

  public ClickHouseLogDAO(String tableName, ConnectionSettings connectionSettings) {
    try {
      this.server = ConnectionSettings.initClickHouseConnection(connectionSettings);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.client = ClickHouseClient.newInstance(server.getProtocol());
    this.tableName = tableName;
  }

  // TODO: provide to define database from connectionsettings?
  //   sqlInjection possible?
  public void insertLogData(String tsvData) throws ClickHouseException
  {
    try (ClickHouseResponse response =
        client
            .write(server)
            .query("INSERT INTO " + tableName)
            .data(new ByteArrayInputStream(tsvData.getBytes(StandardCharsets.UTF_8)))
            .executeAndWait()) {
    }
  }
}
