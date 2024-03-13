package com.crypto.service.dao;

import com.clickhouse.client.*;
import com.clickhouse.data.ClickHouseCompression;
import com.clickhouse.data.ClickHouseFile;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.jdbc.ClickHouseConnection;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

public class ClickHouseDAO {

  private ClickHouseConnection connection;
  private ClickHouseNode server;

  public ClickHouseDAO(ClickHouseConnection connection) {
    this.connection = connection;
  }

  public ClickHouseDAO(ClickHouseNode server) {
    this.server = server;
  }

  public void insertFromFile() {
    try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol())) {
      ClickHouseFile file =
          ClickHouseFile.of(
              "src/main/resources/864400.csv", ClickHouseCompression.NONE, ClickHouseFormat.CSV);
      ClickHouseResponse response =
          client.write(server).table("btc_data").data(file).executeAndWait();
      ClickHouseResponseSummary summary = response.getSummary();
      System.out.println(summary.getWrittenRows());

      response.close();

    } catch (ClickHouseException e) {
      throw new RuntimeException(e);
    }
  }

  public void insertData(List<String> data) {
    try (PreparedStatement statement =
        connection.prepareStatement(
            "INSERT INTO tickets_data SELECT * FROM input('col1 String, col2 UInt64, col3 Float64, "
                + "col4 Float64, col5 Float64, col6 Float64, "
                + "col7 Float64, col8 Float64, col9 DateTime')")) {

      batchInsertData(data, statement);
      statement.executeBatch();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void batchInsertData(List<String> data, PreparedStatement statement) {
    try {
      for (String str : data) {
        String[] values = str.split(",");

        LocalDateTime transaction_time =
          LocalDateTime.ofInstant(
            Instant.ofEpochMilli(Long.parseLong(values[8])), ZoneOffset.UTC);

        statement.setString(1, values[0]);
        statement.setLong(2, Long.parseLong(values[1]));
        statement.setDouble(3, Double.parseDouble(values[2]));
        statement.setDouble(4, Double.parseDouble(values[3]));
        statement.setDouble(5, Double.parseDouble(values[4]));
        statement.setDouble(6, Double.parseDouble(values[5]));
        statement.setDouble(7, Double.parseDouble(values[6]));
        statement.setDouble(8, Double.parseDouble(values[7]));
        statement.setObject(9, transaction_time);

        statement.addBatch();
      }

    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void truncateTable() {
    // JDBC Connection
    if (connection != null) {
      try (PreparedStatement statement = connection.prepareStatement("TRUNCATE TABLE tickets_data")) {
        statement.executeQuery();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    } else if (server != null) {
      try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol())) {
        client
            .read(server)
            .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
            .query("TRUNCATE TABLE tickets_data")
            .executeAndWait();
      } catch (ClickHouseException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void countRecords() {
    // JDBC Connection
    if (connection != null) {
      try (PreparedStatement statement =
          connection.prepareStatement("SELECT COUNT(*) FROM tickets_data")) {
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
          System.out.printf("CURRENT RECORDS IN DATA %d%n", resultSet.getInt(1));
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
      // JavaClient Connection
    } else if (server != null) {
      try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol());
          ClickHouseResponse response =
              client
                  .read(server)
                  .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                  .query("SELECT COUNT(*) FROM tickets_data")
                  .executeAndWait()) {
        Long total_count = response.firstRecord().getValue(0).asLong();
        System.out.printf("CURRENT RECORDS IN DATA %d%n", total_count);
      } catch (ClickHouseException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
