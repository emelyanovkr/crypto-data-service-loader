package com.crypto.service.dao;

import com.clickhouse.client.*;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.*;
import com.clickhouse.data.format.BinaryStreamUtils;
import com.clickhouse.jdbc.ClickHouseConnection;
import com.crypto.service.util.ConnectionHandler;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
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

  private static ClickHouseDAO instance;
  private final ClickHouseNode server;
  private final ClickHouseClient client;

  private ClickHouseDAO() {
    this.server = ConnectionHandler.initJavaClientConnection();
    this.client = ClickHouseClient.newInstance(server.getProtocol());
  }

  public static synchronized ClickHouseDAO getInstance() {
    if (instance == null) {
      instance = new ClickHouseDAO();
    }
    return instance;
  }

  public void insertFromCompressedFileStream(PipedInputStream pin) {
    try (ClickHouseResponse response =
        client
            .write(server)
            .query("INSERT INTO tickets_data_db.tickets_data")
            .data(ClickHousePassThruStream.of(pin ,ClickHouseCompression.GZIP, ClickHouseFormat.CSV))
            .executeAndWait()) {
    } catch (ClickHouseException e) {
      throw new RuntimeException(e);
    }
  }

  public void truncateTable() {
    try (ClickHouseResponse response =
        ClickHouseClient.newInstance(server.getProtocol())
            .read(server)
            .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
            .query("TRUNCATE TABLE tickets_data")
            .executeAndWait()) {

    } catch (ClickHouseException e) {
      throw new RuntimeException(e);
    }
  }

  public void countRecords() {
    try (ClickHouseResponse response =
        ClickHouseClient.newInstance(server.getProtocol())
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
