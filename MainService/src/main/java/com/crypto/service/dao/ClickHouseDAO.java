package com.crypto.service.dao;

import com.clickhouse.client.*;
import com.clickhouse.data.ClickHouseCompression;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHousePassThruStream;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseValue;
import com.crypto.service.data.TickerFile;
import com.crypto.service.util.ConnectionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class ClickHouseDAO {
  private final ClickHouseNode server;
  private final ClickHouseClient client;

  private final Logger LOGGER = LoggerFactory.getLogger(ClickHouseDAO.class);

  public ClickHouseDAO() {
    this.server = ConnectionHandler.initClickHouseConnection();
    this.client = ClickHouseClient.newInstance(server.getProtocol());
  }

  public List<String> selectExclusiveTickerFilesNames(String data, String tableName)
      throws ClickHouseException {
    try (ClickHouseResponse response =
        client
            .read(server)
            .query("SELECT filename FROM :tableName WHERE filename IN (:data)")
            .params(List.of(tableName, data))
            .executeAndWait()) {

      Iterable<ClickHouseRecord> records = response.records();
      List<String> tickerNames = new ArrayList<>();
      for (ClickHouseRecord record : records) {
        ClickHouseValue filenameValue = record.getValue(0);
        tickerNames.add(filenameValue.asString());
      }
      return tickerNames;
    }
  }

  public List<TickerFile> selectTickerFilesOnSpecifiedDate(
      String tableName, String column, LocalDate date) throws ClickHouseException {
    String convertedInputData = "'" + date.toString() + "'";
    try (ClickHouseResponse response =
        client
            .read(server)
            .query("SELECT * FROM :tableName WHERE :column = :date")
            .params(List.of(tableName, column, convertedInputData))
            .executeAndWait()) {
      Iterable<ClickHouseRecord> records = response.records();
      List<TickerFile> tickerFiles = new ArrayList<>();
      for (ClickHouseRecord record : records) {
        String fileRecord = record.getValue(0).asString();
        String[] parsedData = fileRecord.split("\t");
        tickerFiles.add(
            new TickerFile(
                parsedData[0],
                LocalDate.parse(parsedData[1].split(" ")[0]),
                TickerFile.FileStatus.parseStatus(parsedData[2])));
      }
      return tickerFiles;
    }
  }

  public String selectFileStatusOnFilename(String tableName, String filename)
      throws ClickHouseException {
    String sqlFilename = "'" + filename + "'";
    try (ClickHouseResponse response =
        client
            .read(server)
            .query("SELECT status FROM :tableName WHERE filename = :filename")
            .params(List.of(tableName, sqlFilename))
            .executeAndWait()) {

      if (!response.records().iterator().hasNext()) {
        return null;
      }
      return response.firstRecord().getValue(0).asString();
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

      DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

      Iterable<ClickHouseRecord> records = response.records();
      List<TickerFile> tickerNames = new ArrayList<>();
      for (ClickHouseRecord record : records) {
        ClickHouseValue tickerValue = record.getValue(0);
        String[] data = tickerValue.asString().split("\\t");
        LocalDate create_date = LocalDate.parse(data[1], dateTimeFormatter);
        tickerNames.add(
            new TickerFile(data[0], create_date, TickerFile.FileStatus.valueOf(data[2])));
      }
      return tickerNames;
    }
  }

  public LocalDate selectMaxTickerFilesDate(String columnName, String tableName)
      throws ClickHouseException {
    try (ClickHouseResponse response =
        client
            .read(server)
            .query("SELECT MAX(:columName) FROM :tableName")
            .params(List.of(columnName, tableName))
            .executeAndWait()) {
      return response.firstRecord().getValue(0).asDateTime().toLocalDate();
    }
  }

  public LocalDate selectFinishedTickerFilesDate(
      String function, String columnName, String tableName, TickerFile.FileStatus status)
      throws ClickHouseException {
    try (ClickHouseResponse response =
        client
            .read(server)
            .query("SELECT :function (:columnName) FROM :tableName WHERE status = :status")
            .params(List.of(function, columnName, tableName, status.getSQLStatus()))
            .executeAndWait()) {
      return response.firstRecord().getValue(0).asDateTime().toLocalDate();
    }
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

    if (data.isEmpty()) {
      return;
    }

    try (ClickHouseResponse response =
        client
            .write(server)
            .query("ALTER TABLE :tableName UPDATE status = :status WHERE filename IN (:data)")
            .params(List.of(tableName, status.getSQLStatus(), data))
            .executeAndWait()) {}
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
