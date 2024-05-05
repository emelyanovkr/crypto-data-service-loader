package com.crypto.service.dao;

import com.clickhouse.client.*;
import com.clickhouse.data.ClickHouseCompression;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHousePassThruStream;
import com.crypto.service.util.ConnectionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ClickHouseDAO {

  private final ClickHouseNode server;
  private final ClickHouseClient client;

  private final Logger LOGGER = LoggerFactory.getLogger(ClickHouseDAO.class);

  private final AtomicInteger counter = new AtomicInteger(0);

  public ClickHouseDAO() {
    this.server = ConnectionHandler.initClickHouseConnection();
    this.client = ClickHouseClient.newInstance(server.getProtocol());
  }

  public void insertFromCompressedFileStream(PipedInputStream pin, String tableName)
      throws ClickHouseException {

    try (ClickHouseResponse response =
        client
            .write(server)
            .query("INSERT INTO " + tableName)
            .data(
                ClickHousePassThruStream.of(pin, ClickHouseCompression.GZIP, ClickHouseFormat.CSV))
            .executeAndWait()) {
    }
    /*Possible to measure query execution time
    finally {
      LOGGER.info("Query execution time - {} sec.", (System.currentTimeMillis() - start) / 1000);
    }*/
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
