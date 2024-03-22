package com.crypto.service.util;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.data.ClickHouseCompression;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.jdbc.ClickHouseConnection;
import com.crypto.service.dao.ClickHouseDAO;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.checkerframework.checker.units.qual.C;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class TicketsDataReader {
  private ClickHouseDAO clickHouseDAO;

  private final int PARTS_QUANTITY = 32;
  private final int THREADS_COUNT = PARTS_QUANTITY;
  private final String SOURCE_PATH;

  public TicketsDataReader() {
    String currentDate = getCurrentDate();
    SOURCE_PATH = PropertiesLoader.loadProjectConfig().getProperty("DATA_PATH") + "/" + currentDate;
  }

  private String getCurrentDate() {
    LocalDate currentDate = LocalDate.now();
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    return currentDate.format(formatter);
  }

  private List<String> getFilesInDirectory() {
    File searchDirectory = new File(SOURCE_PATH);
    return ImmutableList.copyOf(Objects.requireNonNull(searchDirectory.list())).stream()
        .map(fileName -> Paths.get(SOURCE_PATH, fileName).toString())
        .collect(Collectors.toList());
  }

  /*public void readExecutor() {
    List<String> ticketNames = getFilesInDirectory();

    List<List<String>> ticketParts = Lists.partition(ticketNames, ticketNames.size()/PARTS_QUANTITY);

    try (ClickHouseConnection connection = ConnectionHandler.initJDBCConnection();
        ExecutorService service = Executors.newFixedThreadPool(THREADS_COUNT)) {

      clickHouseDAO = new ClickHouseDAO(connection);
      clickHouseDAO.truncateTable();

      for (List<String> ticketPartition : ticketParts) {
        service.execute(() -> clickHouseDAO.insertFromFile(ticketPartition));
      }

    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }*/

  public void readExecutor() {
    List<String> ticketNames = getFilesInDirectory();

    List<List<String>> ticketParts =
        Lists.partition(ticketNames, ticketNames.size() / PARTS_QUANTITY);

    ClickHouseNode server = ConnectionHandler.initJavaClientConnection();
    ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol());

    try (ExecutorService service = Executors.newFixedThreadPool(THREADS_COUNT)) {

      clickHouseDAO = new ClickHouseDAO(server);
      clickHouseDAO.truncateTable();

      for (List<String> ticketPartition : ticketParts) {
        service.execute(
            () -> {
              for (String fileName : ticketPartition) {
                try (ClickHouseResponse response =
                    client
                        .write(server)
                        .query("INSERT INTO tickets_data_db.tickets_data")
                        .format(ClickHouseFormat.CSV)
                        .data(fileName, ClickHouseCompression.GZIP)
                        .executeAndWait()) {
                } catch (ClickHouseException e) {
                  throw new RuntimeException(e);
                }
              }
            });
      }

      /*for (List<String> ticketPartition : ticketParts) {
        service.execute(new CompressionHandler(ticketPartition));
      }*/
    } finally {
      client.close();
    }
  }
}
