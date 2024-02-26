package com.crypto.service.dao;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.crypto.service.utils.ConnectionHandler;
import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ClickHouseDAO {

  private final ClickHouseConnection connection;

  public ClickHouseDAO(ClickHouseConnection connection) throws SQLException {
    this.connection = connection;
  }

  // there is a problem with using only one statement for all threads
  // race condition situation
  public void insertData(List<String> data) {
    List<List<String>> partitions = Lists.partition(data, 64);

    try (PreparedStatement statement =
        connection.prepareStatement(
            "INSERT INTO btc_data SELECT * FROM input('col1 DateTime, col2 Float32, col3 Float32, "
                + "col4 Float32, col5 Float32, col6 Decimal(38,2), "
                + "col7 DateTime, col8 Float32, col9 Int32, col10 Decimal(38,2), "
                + "col11 Float32, col12 Int32')")) {

      // No difference in using multithread insert or onethread
      for (List<String> subset : partitions) {
        batchInsertData(subset, statement);
      }

      statement.executeBatch();

    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void batchInsertData(List<String> data, PreparedStatement statement) {
    try {
      for (String str : data) {
        String[] values = str.split(",");

        LocalDateTime open_time =
            LocalDateTime.ofInstant(
                Instant.ofEpochMilli(Long.parseLong(values[0])), ZoneOffset.UTC);
        LocalDateTime close_time =
            LocalDateTime.ofInstant(
                Instant.ofEpochMilli(Long.parseLong(values[6])), ZoneOffset.UTC);

        statement.setObject(1, open_time);
        statement.setFloat(2, Float.parseFloat(values[1]));
        statement.setFloat(3, Float.parseFloat(values[2]));
        statement.setFloat(4, Float.parseFloat(values[3]));
        statement.setFloat(5, Float.parseFloat(values[4]));
        statement.setBigDecimal(6, new BigDecimal(values[5]));
        statement.setObject(7, close_time);
        statement.setFloat(8, Float.parseFloat(values[7]));
        statement.setInt(9, Integer.parseInt(values[8]));
        statement.setBigDecimal(10, new BigDecimal(values[9]));
        statement.setFloat(11, Float.parseFloat(values[10]));
        statement.setInt(12, Integer.parseInt(values[11]));

        statement.addBatch();
      }

    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void truncateTable() {
    try (PreparedStatement statement = connection.prepareStatement("TRUNCATE btc_data")) {
      statement.executeQuery();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void countRecords() {
    try (PreparedStatement statement =
        connection.prepareStatement("SELECT COUNT(*) FROM btc_data")) {
      ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        System.out.printf("CURRENT RECORDS IN DATA %d%n", resultSet.getInt(1));
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
