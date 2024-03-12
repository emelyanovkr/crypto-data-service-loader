package com.crypto.service;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.util.ConnectionHandler;
import com.crypto.service.util.TicketsDataReader;

import java.sql.SQLException;
import java.util.List;

public class LoaderApplication {

  public static void main(String[] args) {

    try (ClickHouseConnection connection = ConnectionHandler.initJDBCConnection()) {
      // For JDBC Connection
      ClickHouseDAO clickHouseDAO = new ClickHouseDAO(connection);
      clickHouseDAO.countRecords();


      // For JavaClient Connection
      // ClickHouseNode server = ConnectionHandler.initJavaClientConnection();
      // ClickHouseDAO clickHouseDAO = new ClickHouseDAO(server);
      // clickHouseDAO.insertFromFile();

    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
