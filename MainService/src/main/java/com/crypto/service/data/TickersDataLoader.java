package com.crypto.service.data;

import com.clickhouse.client.ClickHouseException;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.dao.Tables;
import com.crypto.service.util.CompressionHandler;
import com.crypto.service.util.PropertiesLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class TickersDataLoader {
  protected final int PARTS_QUANTITY;
  // 2 THREADS is minimum for using PIPED STREAMS
  protected final int THREADS_COUNT;
  protected final int MAX_FLUSH_ATTEMPTS;

  protected final ExecutorService insert_executor;
  protected final ExecutorService compression_executor;

  protected static final Logger LOGGER = LoggerFactory.getLogger(TickersDataLoader.class);

  protected final String SOURCE_PATH;
  protected ClickHouseDAO clickHouseDAO;

  public TickersDataLoader() {
    String currentDate = getCurrentDate();
    try {
      Properties projectProperties = PropertiesLoader.loadProjectConfig();

      SOURCE_PATH = projectProperties.getProperty("DATA_PATH") + "/" + currentDate;
      MAX_FLUSH_ATTEMPTS = Integer.parseInt(projectProperties.getProperty("max_flush_attempts"));
      PARTS_QUANTITY =
          Integer.parseInt(projectProperties.getProperty("data_divided_parts_quantity"));
      THREADS_COUNT = Math.max(PARTS_QUANTITY / 2, 2);

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
    List<String> tickerNames;

    try {
      tickerNames = List.of(Objects.requireNonNull(searchDirectory.list()));
    } catch (Exception e) {
      LOGGER.error("FAILED TO SEARCH DIRECTORY - ", e);
      throw new RuntimeException();
    }

    return ImmutableList.copyOf(tickerNames).stream()
        .map(fileName -> Paths.get(SOURCE_PATH, fileName).toString())
        .collect(Collectors.toList());
  }

  public void uploadTickersData() {
    List<String> tickerNames = getFilesInDirectory();

    List<List<String>> tickerParts =
        Lists.partition(
            tickerNames,
            tickerNames.size() < (PARTS_QUANTITY) ? 1 : tickerNames.size() / PARTS_QUANTITY);

    for (List<String> tickerPartition : tickerParts) {
      insert_executor.execute(new TickersInsertTask(tickerPartition));
    }
  }

  protected class TickersInsertTask implements Runnable {
    protected final List<String> tickerPartition;
    protected CompressionHandler compressionHandler;
    protected List<TickerFile> tickerFiles;

    public TickersInsertTask(List<String> tickerPartition) {
      this.tickerPartition = tickerPartition;
      this.tickerFiles = new ArrayList<>();
    }

    @Override
    public void run() {
      startInsertTickers();
    }

    protected void fillTickerFileList(List<String> tickerNames) {
      tickerFiles =
          ImmutableList.copyOf(tickerNames).stream()
              .map(
                  fileName ->
                      new TickerFile(
                          fileName.substring(fileName.lastIndexOf('\\') + 1),
                          TickerFile.FileStatus.NOT_LOADED))
              .collect(Collectors.toList());
    }

    protected void startInsertTickers() {
      fillTickerFileList(tickerPartition);

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

          clickHouseDAO.insertTickersData(pin, Tables.TICKERS_DATA.getTableName());
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
      proceedInsertStatus(insertSuccessful);
    }

    protected void proceedInsertStatus(AtomicBoolean insertSuccessful) {
      if (!insertSuccessful.get()) {
        tickerFiles.forEach(tickerFile -> tickerFile.status = TickerFile.FileStatus.ERROR);
        System.err.println(this.getClass().getName() + " LOST TICKERS: ");
      } else {
        tickerFiles.forEach(tickerFile -> tickerFile.status = TickerFile.FileStatus.FINISHED);
      }
      try {
        String dataToInsert =
            tickerFiles.stream()
                .map(tickerFile -> tickerFile.getFileName() + "\t" + tickerFile.getStatus())
                .collect(Collectors.joining("\n"));
        clickHouseDAO.insertTickerFilesInfo(dataToInsert, Tables.TICKER_FILES.getTableName());
      } catch (ClickHouseException e) {
        LOGGER.error("FAILED TO INSERT TICKERS FILES STATUS - ", e);
      }
    }
  }
}
