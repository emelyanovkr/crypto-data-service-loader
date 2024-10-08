package com.crypto.service.data;

import com.crypto.service.MainApplication;
import com.crypto.service.config.TickersDataConfig;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.dao.Tables;
import com.crypto.service.util.CompressionHandler;
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
  protected final int MAX_FLUSH_DATA_ATTEMPTS;
  protected final int SLEEP_ON_RECONNECT_MS;

  protected final ExecutorService insert_executor;
  protected final ExecutorService compression_executor;

  protected static final Logger LOGGER = LoggerFactory.getLogger(TickersDataLoader.class);

  protected final List<Path> filePaths;
  protected List<TickerFile> tickerFiles;
  protected ClickHouseDAO clickHouseDAO;
  protected TickersDataConfig tickersDataConfig;

  public TickersDataLoader(List<Path> filePaths, List<TickerFile> tickerFiles) {
    this.filePaths = new ArrayList<>(filePaths);
    this.tickerFiles = new ArrayList<>(tickerFiles);

    tickersDataConfig = MainApplication.tickersDataConfig;

    MAX_FLUSH_DATA_ATTEMPTS =
        tickersDataConfig.getTickersDataUploaderConfig().getMaxFlushDataAttempts();
    PARTS_QUANTITY = tickersDataConfig.getTickersDataUploaderConfig().getDivideDataPartsQuantity();
    SLEEP_ON_RECONNECT_MS =
        tickersDataConfig.getTickersDataUploaderConfig().getSleepOnReconnectMs();

    THREADS_COUNT = Math.max(PARTS_QUANTITY / 2, 2);

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
    }

    protected void startInsertTickers() {
      compressionTaskRunning = new AtomicBoolean(false);
      for (int i = 0; i < MAX_FLUSH_DATA_ATTEMPTS; i++) {
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

          clickHouseDAO.insertTickersData(pin, Tables.TICKERS_DATA.getTableName());

          taskFuture.set(null);
          break;
        } catch (Exception e) {
          LOGGER.error(
              "FAILED TO INSERT TICKERS DATA, TRYING TO RECONNECT IN {} ms. RETRY #{} EXCEPTION: ",
              SLEEP_ON_RECONNECT_MS,
              i,
              e);

          stopCompressionCommand.set(true);

          try {
            pin.close();
            Thread.sleep(SLEEP_ON_RECONNECT_MS);
          } catch (InterruptedException | IOException ex) {
            throw new RuntimeException(ex);
          }
        }
      }

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
  }
}
