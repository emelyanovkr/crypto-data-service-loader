package com.crypto.service.dao;

import com.clickhouse.client.*;
import com.clickhouse.data.ClickHouseCompression;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHousePassThruStream;
import com.crypto.service.util.ConnectionSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class ClickHouseDAO {

  private static ClickHouseDAO instance;
  private final ClickHouseNode server;
  private final ClickHouseClient client;

  private final Logger LOGGER = LoggerFactory.getLogger(ClickHouseDAO.class);

  private ClickHouseDAO() {
    try {
      this.server = ConnectionSettings.initJavaClientConnection();
    } catch (IOException e) {
      LOGGER.error("FAILED TO ESTABLISH CONNECTION - ", e);
      throw new RuntimeException(e);
    }
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
            .data(
                ClickHousePassThruStream.of(pin, ClickHouseCompression.GZIP, ClickHouseFormat.CSV))
            .executeAndWait()) {
    } catch (ClickHouseException e) {
      LOGGER.error("CLICKHOUSE EXCEPTION - ", e);
      try {
        LOGGER.info("Closing PipedInputStream for worker - {}", Thread.currentThread().getName());
        pin.close();
      } catch (IOException ex) {
        LOGGER.error("FAILED TO CLOSE PipedInputStream - ", ex);
      }
      throw new RuntimeException(e);
    } /* Possible to measure query execution time
      finally {
        LOGGER.info("Query execution time - {} sec.", (System.currentTimeMillis() - start) / 1000);
      }*/
  }

  public void truncateTable(String tableName) {
    try (ClickHouseResponse response =
        ClickHouseClient.newInstance(server.getProtocol())
            .read(server)
            .query("TRUNCATE TABLE " + tableName)
            .executeAndWait()) {
    } catch (ClickHouseException e) {
      LOGGER.error("FAILED TO TRUNCATE TABLE - ", e);
      throw new RuntimeException(e);
    }
  }

  public void countRecords(String tableName) {
    try (ClickHouseResponse response =
        ClickHouseClient.newInstance(server.getProtocol())
            .read(server)
            .query("SELECT COUNT(*) FROM " + tableName)
            .executeAndWait()) {
      Long total_count = response.firstRecord().getValue(0).asLong();
      LOGGER.info("Current records in data - {}", total_count);
    } catch (ClickHouseException e) {
      LOGGER.error("FAILED TO COUNT DATA - ", e);
      throw new RuntimeException(e);
    }
  }
}
