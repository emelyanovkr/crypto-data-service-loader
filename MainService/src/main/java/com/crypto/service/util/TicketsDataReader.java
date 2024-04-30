package com.crypto.service.util;

import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.dao.Tables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class TicketsDataReader {

  private final int PARTS_QUANTITY;
  // 2 THREADS is minimum for using PIPED STREAMS
  private final int THREADS_COUNT;

  private final ExecutorService insert_executor;
  private final ExecutorService compression_executor;

  private final Logger LOGGER = LoggerFactory.getLogger(TicketsDataReader.class);

  private final String SOURCE_PATH;

  private final int FLUSH_RETRY_COUNT;

  public TicketsDataReader() {
    String currentDate = getCurrentDate();
    try {
      Properties projectProperties = PropertiesLoader.loadProjectConfig();

      PARTS_QUANTITY =
          Integer.parseInt(projectProperties.getProperty("data_divided_parts_quantity"));
      THREADS_COUNT = PARTS_QUANTITY / 2;

      SOURCE_PATH = projectProperties.getProperty("DATA_PATH") + "/" + currentDate;
      FLUSH_RETRY_COUNT = Integer.parseInt(projectProperties.getProperty("flush_retry_count"));

    } catch (IllegalArgumentException e) {
      LOGGER.error("FAILED TO PARAMETERS - ", e);
      throw new RuntimeException(e);
    }

    insert_executor = Executors.newFixedThreadPool(THREADS_COUNT);
    compression_executor = Executors.newFixedThreadPool(THREADS_COUNT);
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

    ClickHouseDAO clickHouseDAO = new ClickHouseDAO();

    // TODO: Remove truncation of both tables
    clickHouseDAO.truncateTable(Tables.TICKETS_LOGS.getTableName());
    clickHouseDAO.truncateTable(Tables.TICKETS_DATA.getTableName());

    for (List<String> ticketPartition : ticketParts) {
      insert_executor.execute(
          () -> {
            AtomicBoolean compressionTaskRunning = new AtomicBoolean(false);
            AtomicBoolean insertSuccessful = new AtomicBoolean(false);
            for (int i = 0; i < FLUSH_RETRY_COUNT; i++) {
              AtomicBoolean stopCompressionCommand = new AtomicBoolean(false);

              PipedOutputStream pout = new PipedOutputStream();
              PipedInputStream pin = new PipedInputStream();

              try {
                pin.connect(pout);

                // spinning lock
                while (compressionTaskRunning.get()) {}

                compressionTaskRunning.set(true);

                CompressionHandler handler =
                    new CompressionHandler(pout, compressionTaskRunning, stopCompressionCommand);

                compression_executor.execute(() -> handler.compressFilesWithGZIP(ticketPartition));

                clickHouseDAO.insertFromCompressedFileStream(
                    pin, Tables.TICKETS_DATA.getTableName());
                insertSuccessful.set(true);
                break;
              } catch (Exception e) {
                LOGGER.error("FAILED TO INSERT TICKETS DATA - ", e);

                stopCompressionCommand.set(true);

                try {
                  pin.close();
                  Thread.sleep(500);
                } catch (InterruptedException | IOException ex) {
                  throw new RuntimeException(ex);
                }
              }
            }
            if (!insertSuccessful.get()) {
              System.err.println(this.getClass().getName() + " LOST TICKETS: " );
            }
          });
      //  TODO: After insertion check that COUNT(tickets_logs).equals(partitions) - insert
      //   successful (not reliable)
    }
  }
}
