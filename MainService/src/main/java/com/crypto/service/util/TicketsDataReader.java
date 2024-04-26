package com.crypto.service.util;

import com.clickhouse.client.ClickHouseException;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.dao.Tables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class TicketsDataReader {

  private final int PARTS_QUANTITY = 32;
  // 2 THREADS is minimum for using PIPED STREAMS
  private final int THREADS_COUNT = PARTS_QUANTITY;
  private final String SOURCE_PATH;
  private final Logger LOGGER = LoggerFactory.getLogger(TicketsDataReader.class);
  private final int FLUSH_RETRY_COUNT;

  public TicketsDataReader() {
    String currentDate = getCurrentDate();
    try {
      Properties projectProperties = PropertiesLoader.loadProjectConfig();
      SOURCE_PATH = projectProperties.getProperty("DATA_PATH") + "/" + currentDate;
      FLUSH_RETRY_COUNT = Integer.parseInt(projectProperties.getProperty("flush_retry_count"));
    } catch (IllegalArgumentException e) {
      LOGGER.error("FAILED TO PARAMETERS - ", e);
      throw new RuntimeException(e);
    }
  }

  private String getCurrentDate() {
    LocalDate currentDate = LocalDate.now();
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    return currentDate.format(formatter);
  }

  private List<String> getFilesInDirectory() {
    File searchDirectory = new File(SOURCE_PATH);
    List<String> directories;

    try {
      directories = List.of(Objects.requireNonNull(searchDirectory.list()));
    } catch (Exception e) {
      LOGGER.error("FAILED SEARCH DIRECTORY - ", e);
      throw new RuntimeException();
    }

    return ImmutableList.copyOf(directories).stream()
        .map(fileName -> Paths.get(SOURCE_PATH, fileName).toString())
        .collect(Collectors.toList());
  }

  public void readExecutor() {
    List<String> ticketNames = getFilesInDirectory();

    List<List<String>> ticketParts =
        Lists.partition(ticketNames, ticketNames.size() / (PARTS_QUANTITY));

    // TODO: CHANGE THREADS COUNT
    try (ExecutorService executor = Executors.newFixedThreadPool(THREADS_COUNT - 30)) {

      // TODO: ATOMIC REFERENCE TO USE IN LAMBDAS -> INSERT MANAGER
      AtomicReference<ClickHouseDAO> clickHouseDAO = new AtomicReference<>(new ClickHouseDAO());

      // TODO: Remove truncation of both tables
      clickHouseDAO.get().truncateTable(Tables.TICKETS_LOGS.getTableName());
      clickHouseDAO.get().truncateTable(Tables.TICKETS_DATA.getTableName());

      for (List<String> ticketPartition : ticketParts) {
        PipedOutputStream pout = new PipedOutputStream();
        PipedInputStream pin = new PipedInputStream();
        pin.connect(pout);

        CompressionHandler handler = new CompressionHandler(pout);

        executor.execute(() -> handler.compressFilesWithGZIP(ticketPartition));

        /*executor.execute(
        () ->
        {
          try
          {
            clickHouseDAO
                .get()
                .insertFromCompressedFileStream(pin, Tables.TICKETS_DATA.getTableName());
          } catch (ClickHouseException e)
          {
            throw new RuntimeException(e);
          }
        });*/

        // TODO: REFACTOR HERE?
        //  IMPLEMENT INSERT MANAGER? SYNCHRONIZED?
        executor.execute(
            () -> {
              for (int i = 0; i < FLUSH_RETRY_COUNT; i++) {
                try {
                  clickHouseDAO
                      .get()
                      .insertFromCompressedFileStream(pin, Tables.TICKETS_DATA.getTableName());
                  break;
                } catch (ClickHouseException e) {
                  // LOGGER.error("FAILED TO INSERT TICKETS DATA - ", e);
                  // TODO: PIPE CLOSED WHEN TOO LONG FOR RECOVERING CONNECTION
                  //  FIX CLOSED PIPE!
                  LOGGER.error("EXCEPTION IS: {}", e.getMessage());

                  System.out.println(
                      "TRYING TO RECONNECT..."
                          + " ITERATION : "
                          + i
                          + " "
                          + Thread.currentThread().getName());

                  try {
                    Thread.sleep(3000);
                  } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                  }

                  System.out.println("WAKING UP IN TICKETS " + Thread.currentThread().getName());

                  clickHouseDAO.set(ConnectionHandler.reconnect());

                  if (clickHouseDAO.get() != null) {
                    System.out.println("TESTING CONNECTION...");

                    if (ConnectionHandler.testConnection() != null) {
                      i = 0;
                      System.out.println("SUCCESSFULLY CONNECTED! " + ConnectionHandler.testConnection());
                    }
                  }
                  else
                  {
                    System.out.println(
                      "CLICKHOUSE DAO IS NULL, TRYING TO RECONNECT: "
                        + i
                        + " "
                        + Thread.currentThread().getName());
                  }
                  /*try {
                    LOGGER.info("CLOSING PIPED STREAM");
                    pin.close();
                  } catch (IOException ex) {
                    LOGGER.error("FAILED TO CLOSING PIPED STREAM - ", ex);
                    throw new RuntimeException(ex);
                  }*/
                }
              }
            });

        //  TODO: After insertion check that COUNT(tickets_logs).equals(partitions) - insert
        //   successful (not reliable)
      }

    } catch (IOException e) {
      LOGGER.error("FAILED TO CONNECT PIPED STREAMS - ", e);
      throw new RuntimeException(e);
    }
  }
}
