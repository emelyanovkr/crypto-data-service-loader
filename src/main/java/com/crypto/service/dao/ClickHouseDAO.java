package com.crypto.service.dao;

import com.clickhouse.client.*;
import com.clickhouse.data.*;
import com.crypto.service.util.ConnectionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PipedInputStream;

public class ClickHouseDAO {

  private static ClickHouseDAO instance;
  private final ClickHouseNode server;
  private final ClickHouseClient client;

  private final Logger LOGGER = LoggerFactory.getLogger(ClickHouseDAO.class);
  private ClickHouseDAO() {
    try {
      this.server = ConnectionHandler.initJavaClientConnection();
    } catch (IOException e) {
      LOGGER.error("FAILED TO ESTABLISH CONNECTION - {}", e.getMessage());
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
      LOGGER.error("CLICKHOUSE EXCEPTION - {}", e.getMessage());
      try {
        LOGGER.info("Closing PipedInputStream for worker - {}", Thread.currentThread().getName());
        pin.close();
      } catch (IOException ex) {
        LOGGER.error("FAILED TO CLOSE PipedInputStream - {}", ex.getMessage());
      }
      throw new RuntimeException(e);
    } /* Possible to measure query execution time
      finally {
        LOGGER.info("Query execution time - {} sec.", (System.currentTimeMillis() - start) / 1000);
      }*/
  }

  public void truncateTable() {
    try (ClickHouseResponse response =
        ClickHouseClient.newInstance(server.getProtocol())
            .read(server)
            .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
            .query("TRUNCATE TABLE tickets_data")
            .executeAndWait()) {
    } catch (ClickHouseException e) {
      LOGGER.error("FAILED TO TRUNCATE TABLE - {}", e.getMessage());
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
      LOGGER.info("Current records in data - {}", total_count);
    } catch (ClickHouseException e) {
      LOGGER.error("FAILED TO COUNT DATA - {}", e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
