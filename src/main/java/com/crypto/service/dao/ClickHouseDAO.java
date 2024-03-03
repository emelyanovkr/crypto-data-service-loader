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
          client
              .write(server)
              .table("btc_data")
              .data(file)
              .executeAndWait();
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
            "INSERT INTO btc_data SELECT * FROM input('col1 DateTime, col2 Float32, col3 Float32, "
                + "col4 Float32, col5 Float32, col6 Decimal(38,2), "
                + "col7 DateTime, col8 Float32, col9 Int32, col10 Decimal(38,2), "
                + "col11 Float32, col12 Int32')")) {

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
    // JDBC Connection
    if (connection != null) {
      try (PreparedStatement statement = connection.prepareStatement("TRUNCATE TABLE btc_data")) {
        statement.executeQuery();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    } else if (server != null) {
      try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol())) {
        client
            .read(server)
            .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
            .query("TRUNCATE TABLE btc_data")
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
          connection.prepareStatement("SELECT COUNT(*) FROM btc_data")) {
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
                  .query("SELECT COUNT(*) FROM btc_data")
                  .executeAndWait()) {
        Long total_count = response.firstRecord().getValue(0).asLong();
        System.out.printf("CURRENT RECORDS IN DATA %d%n", total_count);
      } catch (ClickHouseException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
