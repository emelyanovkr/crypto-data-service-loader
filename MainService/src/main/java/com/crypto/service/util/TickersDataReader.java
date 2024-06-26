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

public class TickersDataReader {
  protected final int PARTS_QUANTITY;
  // 2 THREADS is minimum for using PIPED STREAMS
  protected final int THREADS_COUNT;
  protected final int MAX_FLUSH_ATTEMPTS;

  protected final ExecutorService insert_executor;
  protected final ExecutorService compression_executor;

  protected final Logger LOGGER = LoggerFactory.getLogger(TickersDataReader.class);

  protected final String SOURCE_PATH;
  protected ClickHouseDAO clickHouseDAO;

  public TickersDataReader() {
    String currentDate = getCurrentDate();
    try {
      Properties projectProperties = PropertiesLoader.loadProjectConfig();

      PARTS_QUANTITY =
          Integer.parseInt(projectProperties.getProperty("data_divided_parts_quantity"));
      THREADS_COUNT = Math.max(PARTS_QUANTITY / 2, 2);

      SOURCE_PATH = projectProperties.getProperty("DATA_PATH") + "/" + currentDate;
      MAX_FLUSH_ATTEMPTS = Integer.parseInt(projectProperties.getProperty("max_flush_attempts"));

    } catch (IllegalArgumentException e) {
      LOGGER.error("FAILED TO READ PARAMETERS - ", e);
      throw new RuntimeException(e);
    }

    insert_executor = Executors.newFixedThreadPool(THREADS_COUNT);
    compression_executor = Executors.newFixedThreadPool(THREADS_COUNT);

    clickHouseDAO = new ClickHouseDAO();
  }

  protected String getCurrentDate() {
    LocalDate currentDate = LocalDate.now();
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    return currentDate.format(formatter);
  }

  protected List<String> getFilesInDirectory() {
    File searchDirectory = new File(SOURCE_PATH);
    List<String> directories;

    try {
      directories = List.of(Objects.requireNonNull(searchDirectory.list()));
    } catch (Exception e) {
      LOGGER.error("FAILED TO SEARCH DIRECTORY - ", e);
      throw new RuntimeException();
    }

    return ImmutableList.copyOf(directories).stream()
        .map(fileName -> Paths.get(SOURCE_PATH, fileName).toString())
        .collect(Collectors.toList());
  }

  public void readExecutor() {
    List<String> tickerNames = getFilesInDirectory();

    List<List<String>> tickerParts =
        Lists.partition(
            tickerNames,
            tickerNames.size() < (PARTS_QUANTITY) ? 1 : tickerNames.size() / PARTS_QUANTITY);

    // TODO: Remove truncation of both tables
    clickHouseDAO.truncateTable(Tables.TICKERS_LOGS.getTableName());
    clickHouseDAO.truncateTable(Tables.TICKERS_DATA.getTableName());

    for (List<String> tickerPartition : tickerParts) {
      insert_executor.execute(new TickersInsertTask(tickerPartition));
    }

    //  TODO: After insertion check that COUNT(tickers_logs).equals(partitions) - insert
    //   successful (not reliable)
  }

  protected class TickersInsertTask implements Runnable {
    protected final List<String> tickerPartition;
    protected CompressionHandler compressionHandler;

    public TickersInsertTask(List<String> tickerPartition) {
      this.tickerPartition = tickerPartition;
    }

    @Override
    public void run() {
      startInsertTickers();
    }

    protected void startInsertTickers() {
      AtomicBoolean compressionTaskRunning = new AtomicBoolean(false);
      AtomicBoolean insertSuccessful = new AtomicBoolean(false);
      for (int i = 0; i < MAX_FLUSH_ATTEMPTS; i++) {
        AtomicBoolean stopCompressionCommand = new AtomicBoolean(false);

        PipedOutputStream pout = new PipedOutputStream();
        PipedInputStream pin = new PipedInputStream();

        try {
          pin.connect(pout);

          // spinning lock
          while (compressionTaskRunning.get()) {}

          compressionTaskRunning.set(true);

          compressionHandler =
              CompressionHandler.createCompressionHandler(
                  pout, compressionTaskRunning, stopCompressionCommand);

          compression_executor.execute(
              () -> compressionHandler.compressFilesWithGZIP(tickerPartition));

          clickHouseDAO.insertFromCompressedFileStream(pin, Tables.TICKERS_DATA.getTableName());
          insertSuccessful.set(true);
          break;
        } catch (Exception e) {
          LOGGER.error("FAILED TO INSERT TICKERS DATA - ", e);

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
        System.err.println(this.getClass().getName() + " LOST TICKERS: ");
      }
    }
  }
}
