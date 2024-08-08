package com.crypto.service.data;

import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.dao.Tables;
import com.crypto.service.util.CompressionHandler;
import com.crypto.service.util.PropertiesLoader;
import com.crypto.service.util.WorkersUtil;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
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

  public ListenableFuture<Map<ListenableFuture<Void>, List<TickerFile>>> uploadTickersData() {

    List<List<Path>> tickerPathPartition =
        Lists.partition(
            filePaths, filePaths.size() < (PARTS_QUANTITY) ? 1 : filePaths.size() / PARTS_QUANTITY);

    List<List<TickerFile>> tickerFilesPartition =
        Lists.partition(
            tickerFiles,
            tickerFiles.size() < (PARTS_QUANTITY) ? 1 : tickerFiles.size() / PARTS_QUANTITY);

    Map<ListenableFuture<Void>, List<TickerFile>> taskFutures = new HashMap<>();
    for (int i = 0; i < tickerPathPartition.size(); i++) {
      SettableFuture<Void> taskFuture = SettableFuture.create();
      List<TickerFile> filesPartition = tickerFilesPartition.get(i);
      insert_executor.execute(
          new TickersInsertTask(tickerPathPartition.get(i), filesPartition, taskFuture));
      taskFutures.put(taskFuture, filesPartition);
    }

    SettableFuture<Map<ListenableFuture<Void>, List<TickerFile>>> mainFuture =
        SettableFuture.create();

    Futures.whenAllComplete(taskFutures.keySet())
        .run(() -> mainFuture.set(taskFutures), MoreExecutors.directExecutor());

    return mainFuture;
  }

  protected class TickersInsertTask implements Runnable {
    protected final List<Path> tickerPathPartition;
    protected final List<TickerFile> tickerFilesPartition;

    protected CompressionHandler compressionHandler;

    protected AtomicBoolean compressionTaskRunning;
    protected final SettableFuture<Void> taskFuture;

    public TickersInsertTask(
        List<Path> tickerPathPartition,
        List<TickerFile> tickerFilesPartition,
        SettableFuture<Void> taskFuture) {
      this.tickerPathPartition = tickerPathPartition;
      this.tickerFilesPartition = tickerFilesPartition;
      this.taskFuture = taskFuture;
    }

    @Override
    public void run() {
      startInsertTickers();
      proceedInsertStatus();
    }

    protected void startInsertTickers() {
      compressionTaskRunning = new AtomicBoolean(false);
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

          taskFuture.set(null);
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

      // TODO: дублирующаяся часть кода, надо подумать, что с ней делать
      if (!taskFuture.isDone()) {
        taskFuture.setException(new Exception("FAILED TO INSERT TICKERS DATA"));
        tickerFilesPartition.forEach(
            tickerFile -> {
              if (tickerFile.status != TickerFile.FileStatus.FINISHED) {
                tickerFile.status = TickerFile.FileStatus.ERROR;
              }
            });
        System.err.println(this.getClass().getName() + " LOST TICKERS");
      }
    }

    protected void proceedInsertStatus() {
      try {
        taskFuture.get();
        tickerFilesPartition.forEach(
            tickerFile -> {
              if (tickerFile.status != TickerFile.FileStatus.FINISHED) {
                tickerFile.status = TickerFile.FileStatus.ERROR;
              }
            });
      } catch (Exception ignored) {
      }

      WorkersUtil.changeTickerFileUpdateStatus(
          clickHouseDAO, tickerFilesPartition, TickerFile.FileStatus.FINISHED);
      WorkersUtil.changeTickerFileUpdateStatus(
          clickHouseDAO, tickerFilesPartition, TickerFile.FileStatus.ERROR);
    }
  }
}
