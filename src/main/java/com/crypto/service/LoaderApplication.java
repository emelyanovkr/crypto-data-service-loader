package com.crypto.service;

import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseNodes;
import com.clickhouse.jdbc.ClickHouseConnection;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.utils.ConnectionHandler;
import com.crypto.service.utils.SourceReader;

import java.sql.SQLException;
import java.util.List;

public class LoaderApplication {

  public static void main(String[] args) {

    List<String> data = SourceReader.readBigData();

    try (ClickHouseConnection connection = ConnectionHandler.initJDBCConnection()) {
      // For JDBC Connection
      ClickHouseDAO clickHouseDAO = new ClickHouseDAO(connection);

      // For JavaClient Connection
      ClickHouseNode server = ConnectionHandler.initJavaClientConnection();
      clickHouseDAO.query(server);
      // clickHouseDAO.insertData(data);
      // clickHouseDAO.insertFromFile();

    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
