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
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
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

  protected List<String> getFilesInDirectory(String directory) {
    File searchDirectory = new File(directory);
    List<String> tickerNames;

    try {
      tickerNames = List.of(Objects.requireNonNull(searchDirectory.list()));
    } catch (Exception e) {
      LOGGER.error("FAILED TO SEARCH DIRECTORY - ", e);
      throw new RuntimeException();
    }

    return ImmutableList.copyOf(tickerNames).stream()
        .map(fileName -> Paths.get(directory, fileName).toString())
        .collect(Collectors.toList());
  }

  public void uploadTickersData() {
    List<String> tickerNames = getFilesInDirectory(SOURCE_PATH);

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
    protected Map<String, TickerFile> tickerFilesMap;

    public TickersInsertTask(List<String> tickerPartition) {
      this.tickerPartition = tickerPartition;
      this.tickerFilesMap = new HashMap<>();
    }

    @Override
    public void run() {
      startInsertTickers();
    }

    // TODO: FLOW 3
    /*    protected void fillTickerFileList(List<String> tickerNames) {
      tickerFilesMap =
          ImmutableList.copyOf(tickerNames).stream()
              .collect(
                  Collectors.toMap(
                      filePath -> filePath,
                      filePath ->
                          new TickerFile(
                              filePath.substring(filePath.lastIndexOf('\\') + 1),
                              TickerFile.FileStatus.NOT_LOADED)));
    }*/

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

          // TODO: UPDATE статусы для всех файлов в IN_PROGRESS

          // TODO: передавать структуру с tickerFiles, для каждого файла я установлю соответствующий
          // статус
          compression_executor.execute(
              () ->
                  compressionHandler.compressFilesWithGZIP(
                      tickerPartition,
                      (fileStatus, filePath) -> {
                        tickerFilesMap.get(filePath).setStatus(fileStatus);
                        return null;
                      }));

          // TODO: проверить, вставляются ли данные в середине работы программы, в случае если вся
          // пачка упадёт
          // для тех кто обработался должен быть установлен статус finished, для всех остальных
          // error
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
      /*
        проверить, что статус != error ->
      */

      if (!insertSuccessful.get()) {
        tickerFilesMap
            .values()
            .forEach(
                tickerFile -> {
                  if (tickerFile.status != TickerFile.FileStatus.FINISHED) {
                    tickerFile.status = TickerFile.FileStatus.ERROR;
                  }
                });
        System.err.println(this.getClass().getName() + " LOST TICKERS: ");
      }

      String dataToInsert = TickerFile.formDataToInsert(tickerFilesMap.values());
      // TODO: change insert to UPDATE STATUS IN DB ON FINISHED OR ERROR

    }
  }
}
