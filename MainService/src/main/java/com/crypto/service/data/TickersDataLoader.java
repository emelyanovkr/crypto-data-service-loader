package com.crypto.service.data;

import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.dao.Tables;
import com.crypto.service.util.CompressionHandler;
import com.crypto.service.util.PropertiesLoader;
import com.crypto.service.util.WorkersUtil;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class TickersDataLoader {
  protected final int PARTS_QUANTITY;
  // 2 THREADS is minimum for using PIPED STREAMS
  protected final int THREADS_COUNT;
  protected final int MAX_FLUSH_ATTEMPTS;

  protected final ExecutorService insert_executor;
  protected final ExecutorService compression_executor;

  protected static final Logger LOGGER = LoggerFactory.getLogger(TickersDataLoader.class);

  protected final List<Path> filePaths;
  protected List<TickerFile> tickerFiles;
  protected ClickHouseDAO clickHouseDAO;

  public TickersDataLoader(List<Path> filePaths, List<TickerFile> tickerFiles) {
    this.filePaths = new ArrayList<>(filePaths);
    this.tickerFiles = new ArrayList<>(tickerFiles);

    try {
      Properties projectProperties = PropertiesLoader.loadProjectConfig();
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

    this.clickHouseDAO = new ClickHouseDAO();
  }

  public void uploadTickersData() {

    List<List<Path>> tickerPathPartition =
        Lists.partition(
            filePaths, filePaths.size() < (PARTS_QUANTITY) ? 1 : filePaths.size() / PARTS_QUANTITY);

    List<List<TickerFile>> tickerFilesPartition =
        Lists.partition(
            tickerFiles,
            tickerFiles.size() < (PARTS_QUANTITY) ? 1 : tickerFiles.size() / PARTS_QUANTITY);

    for (int i = 0; i < tickerPathPartition.size(); i++) {
      insert_executor.execute(
          new TickersInsertTask(tickerPathPartition.get(i), tickerFilesPartition.get(i)));
    }
  }

  protected class TickersInsertTask implements Runnable {
    protected final List<Path> tickerPathPartition;
    protected final List<TickerFile> tickerFilesPartition;

    protected CompressionHandler compressionHandler;

    protected AtomicBoolean compressionTaskRunning;
    protected AtomicBoolean insertSuccessful;

    public TickersInsertTask(
        List<Path> tickerPathPartition, List<TickerFile> tickerFilesPartition) {
      this.tickerPathPartition = tickerPathPartition;
      this.tickerFilesPartition = tickerFilesPartition;
    }

    @Override
    public void run() {
      startInsertTickers();
      proceedInsertStatus(insertSuccessful);
    }

    protected void startInsertTickers() {
      compressionTaskRunning = new AtomicBoolean(false);
      insertSuccessful = new AtomicBoolean(false);
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
              () ->
                  compressionHandler.compressFilesWithGZIP(
                      tickerPathPartition, new HashSet<>(tickerFilesPartition)));

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
    }

    // TODO: possible get out in UploaderWorker?
    protected void proceedInsertStatus(AtomicBoolean insertSuccessful) {
      if (!insertSuccessful.get()) {
        tickerFilesPartition.forEach(
            tickerFile -> {
              if (tickerFile.status != TickerFile.FileStatus.FINISHED) {
                tickerFile.status = TickerFile.FileStatus.ERROR;
              }
            });
        System.err.println(this.getClass().getName() + " LOST TICKERS: ");
      }

      WorkersUtil.proceedToUpdateStatus(
          clickHouseDAO, tickerFilesPartition, TickerFile.FileStatus.FINISHED);
      WorkersUtil.proceedToUpdateStatus(
          clickHouseDAO, tickerFilesPartition, TickerFile.FileStatus.ERROR);
    }
  }
}
