package com.crypto.service.dao;

import com.clickhouse.client.*;
import com.clickhouse.data.ClickHouseCompression;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHousePassThruStream;
import com.clickhouse.data.ClickHouseRecord;
import com.crypto.service.data.TickerFile;
import com.crypto.service.util.ConnectionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.StreamSupport;

public class ClickHouseDAO {
  private final ClickHouseNode server;
  private final ClickHouseClient client;

  private final Logger LOGGER = LoggerFactory.getLogger(ClickHouseDAO.class);

  public ClickHouseDAO() {
    this.server = ConnectionHandler.initClickHouseConnection();
    this.client = ClickHouseClient.newInstance(server.getProtocol());
  }

  public void insertTickersData(PipedInputStream pin, String tableName) throws ClickHouseException {
    try (ClickHouseResponse response =
        client
            .write(server)
            .query("INSERT INTO " + tableName)
            .data(
                ClickHousePassThruStream.of(pin, ClickHouseCompression.GZIP, ClickHouseFormat.CSV))
            .executeAndWait()) {}
    /*Possible to measure query execution time
    finally {
      LOGGER.info("Query execution time - {} sec.", (System.currentTimeMillis() - start) / 1000);
    }*/
  }

  public void insertTickerFilesInfo(String data, String tableName) throws ClickHouseException {
    try (ClickHouseResponse response =
        client
            .write(server)
            .query("INSERT INTO " + tableName)
            .data(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)))
            .executeAndWait()) {}
  }

  public void updateTickerFilesStatus(String data, TickerFile.FileStatus status, String tableName)
      throws ClickHouseException {
    try (ClickHouseResponse response =
        client
            .write(server)
            .query("ALTER TABLE :tableName UPDATE status = :status WHERE filename IN (:data)")
            .params(List.of(tableName, status.getSQLStatus(), data))
            .executeAndWait()) {}
  }

  public List<String> selectTickerFilesNames(String tableName) throws ClickHouseException {
    try (ClickHouseResponse response =
        client
            .read(server)
            .query("SELECT filename FROM :tableName")
            .params(tableName)
            .executeAndWait()) {

      // TODO: REFACTOR
      Iterable<ClickHouseRecord> records = response.records();
      List<String> tickerNames = new ArrayList<>();
      for (ClickHouseRecord record : records) {
        tickerNames.add(record.iterator().next().asString());
      }
      return tickerNames;
    }
  }

  public List<TickerFile> selectTickerFilesNamesOnStatus(
      String tableName, TickerFile.FileStatus... statuses) throws ClickHouseException {
    StringJoiner joiner = new StringJoiner(",");

    for (TickerFile.FileStatus status : statuses) {
      joiner.add(status.getSQLStatus());
    }

    try (ClickHouseResponse response =
        client
            .read(server)
            .query("SELECT * FROM :tableName WHERE status IN (:data)")
            .params(List.of(tableName, joiner.toString()))
            .executeAndWait()) {

      // TODO: REFACTOR
      Iterable<ClickHouseRecord> records = response.records();
      List<TickerFile> tickerNames = new ArrayList<>();
      for (ClickHouseRecord record : records) {
        String[] data = record.iterator().next().asString().split("\\t");
        tickerNames.add(new TickerFile(data[0], TickerFile.FileStatus.valueOf(data[1])));
      }
      return tickerNames;
    }
  }

  public void truncateTable(String tableName) {
    try (ClickHouseResponse response =
        client.read(server).query("TRUNCATE TABLE " + tableName).executeAndWait()) {
    } catch (ClickHouseException e) {
      LOGGER.error("FAILED TO TRUNCATE TABLE - ", e);
      throw new RuntimeException(e);
    }
  }

  public void countRecords(String tableName) {
    try (ClickHouseResponse response =
        client.read(server).query("SELECT COUNT(*) FROM " + tableName).executeAndWait()) {
      Long total_count = response.firstRecord().getValue(0).asLong();
      LOGGER.info("Current records in data - {}", total_count);
    } catch (ClickHouseException e) {
      LOGGER.error("FAILED TO COUNT DATA - ", e);
      throw new RuntimeException(e);
    }
  }
}
