package com.crypto.service.dao;

import com.clickhouse.client.*;
import com.clickhouse.data.*;
import com.clickhouse.data.format.BinaryStreamUtils;
import com.clickhouse.jdbc.ClickHouseConnection;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ClickHouseDAO {

  private ClickHouseConnection connection;
  private ClickHouseNode server;

  public ClickHouseDAO(ClickHouseConnection connection) {
    this.connection = connection;
  }

  public ClickHouseDAO(ClickHouseNode server) {
    this.server = server;
  }

  /* public void insertClient(List<String> data) {
    try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol());
    ClickHouseResponse response = client.read(server).write()
      .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
      .query("INSERT INTO tickets_data SELECT * FROM input('c1 String, c2 UInt64, c3 Float64, "
        + "c4 Float64, c5 Float64, c6 Float64, "
        + "c7 Float64, c8 Float64, c9 DateTime')")
      .params(1)
      .executeAndWait()) {
      ClickHouseResponseSummary summary = response.getSummary();
      System.out.println(summary.getWrittenRows());
    } catch (ClickHouseException e)
    {
      throw new RuntimeException(e);
    }
  }*/

  public void insertClient(List<String> data) {
    try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol())) {
      ClickHouseRequest.Mutation request =
          client
              .read(server)
              .write()
              .table("tickets_data_db.tickets_data")
              .format(ClickHouseFormat.RowBinary);
      ClickHouseConfig config = request.getConfig();
      try (ClickHouseResponse response =
          request
              .write()
              .format(ClickHouseFormat.RowBinary)
              .query(
                  "INSERT INTO tickets_data SELECT * FROM input('c1 String, c2 UInt64, c3 Float64, "
                      + "c4 Float64, c5 Float64, c6 Float64, "
                      + "c7 Float64, c8 Float64, c9 DateTime')")
              .data(
                  output -> {
                    List<ClickHouseColumn> columns =
                        ClickHouseColumn.parse(
                            "ticketName String, sequence UInt64, price Float64, size Float64, bestAsk Float64, bestAskSize Float64, bestBid Float64, bestBidSize Float64, transactionTime DateTime");
                    ClickHouseValue[] values = ClickHouseValues.newValues(config, columns);
                    ClickHouseDataProcessor processor =
                        ClickHouseDataStreamFactory.getInstance()
                            .getProcessor(config, null, output, null, columns);
                    ClickHouseSerializer[] serializers = processor.getSerializers(config, columns);

                    for (String str : data) {
                      String[] line_values = str.split(",");

                      LocalDateTime transaction_time =
                          LocalDateTime.ofInstant(
                              Instant.ofEpochMilli(Long.parseLong(line_values[8])), ZoneOffset.UTC);

                      values[0].update(line_values[0]);
                      values[1].update(Long.parseLong(line_values[1]));
                      values[2].update(Float.parseFloat(line_values[2]));
                      values[3].update(Float.parseFloat(line_values[3]));
                      values[4].update(Float.parseFloat(line_values[4]));
                      values[5].update(Float.parseFloat(line_values[5]));
                      values[6].update(Float.parseFloat(line_values[6]));
                      values[7].update(Float.parseFloat(line_values[7]));
                      values[8].update(transaction_time);

                      for (int i = 0; i < line_values.length; ++i) {
                        serializers[i].serialize(values[i], output);
                      }
                    }
                  })
              .executeAndWait()) {
        ClickHouseResponseSummary summary = response.getSummary();
        System.out.println(summary.getWrittenRows());
      } catch (ClickHouseException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void insertData(List<String> data) {
    try (PreparedStatement statement =
        connection.prepareStatement(
            "INSERT INTO tickets_data_db.tickets_data SELECT * FROM input('col1 String, col2 UInt64, col3 Float64, "
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
      try (PreparedStatement statement =
          connection.prepareStatement("TRUNCATE TABLE tickets_data_db.tickets_data")) {
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
          connection.prepareStatement("SELECT COUNT(*) FROM tickets_data_db.tickets_data")) {
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
