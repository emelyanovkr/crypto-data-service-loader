package com.crypto.service.util;

import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.jdbc.ClickHouseConnection;
import com.crypto.service.dao.ClickHouseDAO;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.checkerframework.checker.units.qual.C;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
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
    return ImmutableList.copyOf(Objects.requireNonNull(searchDirectory.list()));
  }

  public void readExecutor() {
    List<String> ticketNames = getFilesInDirectory();

    List<List<String>> ticketParts = Lists.partition(ticketNames, ticketNames.size()/PARTS_QUANTITY);

    try (ClickHouseConnection connection = ConnectionHandler.initJDBCConnection();
        ExecutorService service = Executors.newFixedThreadPool(THREADS_COUNT)) {

      clickHouseDAO = new ClickHouseDAO(connection);
      clickHouseDAO.truncateTable();
      clickHouseDAO.countRecords();

      for (List<String> ticketPartition : ticketParts) {
        service.execute(new FileProcessor(ticketPartition));
      }

    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  class FileProcessor implements Runnable {
    private final List<String> partition;

    public FileProcessor(List<String> partition) {
      this.partition = partition;
    }

    @Override
    public void run() {
      for (String fileName : partition) {
        collectData(fileName);
      }
    }

    private void collectData(String fileName) {
      try {
        List<String> ticketInfo = Files.readAllLines(Paths.get(SOURCE_PATH + "/" + fileName));
        clickHouseDAO.insertData(ticketInfo);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
