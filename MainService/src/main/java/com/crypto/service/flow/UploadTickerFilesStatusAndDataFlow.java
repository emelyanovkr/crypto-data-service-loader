package com.crypto.service.flow;

import com.clickhouse.client.ClickHouseException;
import com.crypto.service.MainApplication;
import com.crypto.service.config.MainFlowsConfig;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.dao.Tables;
import com.crypto.service.data.TickerFile;
import com.crypto.service.data.TickersDataLoader;
import com.crypto.service.util.WorkersUtil;
import com.flower.anno.flow.FlowType;
import com.flower.anno.flow.State;
import com.flower.anno.functions.SimpleStepFunction;
import com.flower.anno.params.common.In;
import com.flower.anno.params.common.Out;
import com.flower.anno.params.transit.StepRef;
import com.flower.conf.OutPrm;
import com.flower.conf.Transition;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

// FLOW 3
@FlowType(firstStep = "RETRIEVE_PREPARED_FILES")
public class UploadTickerFilesStatusAndDataFlow {

  protected static final Logger LOGGER =
      LoggerFactory.getLogger(UploadTickerFilesStatusAndDataFlow.class);

  protected final MainFlowsConfig mainFlowsConfig;
  protected static int WORK_CYCLE_TIME_SEC;

  @State protected String directoryPath;
  @State protected ClickHouseDAO clickHouseDAO;
  @State protected List<TickerFile> tickerFiles;
  @State protected List<Path> filePaths;

  public UploadTickerFilesStatusAndDataFlow(MainFlowsConfig mainFlowsConfig, String directoryPath) {
    this.mainFlowsConfig = mainFlowsConfig;
    WORK_CYCLE_TIME_SEC = mainFlowsConfig.getUploadTickersDataConfig().getWorkCycleTimeSec();

    this.directoryPath = directoryPath;
    this.clickHouseDAO = new ClickHouseDAO();
    this.tickerFiles = new ArrayList<>();
  }

  @SimpleStepFunction
  public static Transition RETRIEVE_PREPARED_FILES(
      @Out OutPrm<List<TickerFile>> tickerFiles,
      @In(throwIfNull = true) ClickHouseDAO clickHouseDAO,
      @StepRef Transition FILL_PATHS_LIST) {
    try {

      List<TickerFile> tickerFilesVal =
          clickHouseDAO.selectTickerFilesNamesOnStatus(
              Tables.TICKER_FILES.getTableName(), TickerFile.FileStatus.READY_FOR_PROCESSING);

      tickerFiles.setOutValue(tickerFilesVal);

      clickHouseDAO.updateTickerFilesStatus(
          TickerFile.getSQLFileNames(tickerFilesVal),
          TickerFile.FileStatus.IN_PROGRESS,
          Tables.TICKER_FILES.getTableName());

    } catch (ClickHouseException e) {
      throw new RuntimeException(e);
    }
    return FILL_PATHS_LIST;
  }

  @SimpleStepFunction
  public static Transition FILL_PATHS_LIST(
      @In List<TickerFile> tickerFiles,
      @Out OutPrm<List<Path>> filePaths,
      @In(throwIfNull = true) String directoryPath,
      @StepRef Transition UPLOAD_TICKERS_FILES_DATA) {
    List<Path> filePathsVal = new ArrayList<>();
    filePaths.setOutValue(filePathsVal);

    HashMap<String, HashSet<String>> tickerDatesToFileNames =
        tickerFiles.stream()
            .collect(
                Collectors.groupingBy(
                    tickerFile -> tickerFile.getCreateDate().toString(),
                    HashMap::new,
                    Collectors.mapping(
                        TickerFile::getFileName, Collectors.toCollection(HashSet::new))));

    try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Path.of(directoryPath))) {
      for (Path pathEntry : directoryStream) {
        String createDateDirectory = pathEntry.getFileName().toString();

        if (Files.isDirectory(pathEntry)
            && tickerDatesToFileNames.containsKey(createDateDirectory)) {
          try (DirectoryStream<Path> innerDirectoryStream = Files.newDirectoryStream(pathEntry)) {
            for (Path innerPathEntry : innerDirectoryStream) {
              if (tickerDatesToFileNames
                  .get(createDateDirectory)
                  .contains(innerPathEntry.getFileName().toString())) {
                filePathsVal.add(innerPathEntry.toAbsolutePath());
              }
            }
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return UPLOAD_TICKERS_FILES_DATA;
  }

  @SimpleStepFunction
  public static ListenableFuture<Transition> UPLOAD_TICKERS_FILES_DATA(
      @In(throwIfNull = true) ClickHouseDAO clickHouseDAO,
      @In List<TickerFile> tickerFiles,
      @In List<Path> filePaths,
      @StepRef Transition RETRIEVE_PREPARED_FILES) {

    if (tickerFiles.isEmpty()) {
      return Futures.immediateFuture(
          RETRIEVE_PREPARED_FILES.setDelay(Duration.ofSeconds(WORK_CYCLE_TIME_SEC)));
    }

    TickersDataLoader dataLoader = new TickersDataLoader(filePaths, tickerFiles);

    LOGGER.info("Ready to upload {} tickers files data", tickerFiles.size());
    long uploadingStartTime = System.currentTimeMillis();

    ListenableFuture<Map<ListenableFuture<Void>, List<TickerFile>>> uploadTickerFuture =
        dataLoader.uploadTickersData();

    return Futures.transform(
        uploadTickerFuture,
        map -> {
          for (Map.Entry<ListenableFuture<Void>, List<TickerFile>> ent : map.entrySet()) {
            ListenableFuture<Void> future = ent.getKey();
            try {
              future.get();
              WorkersUtil.changeTickerFileUpdateStatus(
                  clickHouseDAO, ent.getValue(), TickerFile.FileStatus.FINISHED);
              WorkersUtil.changeTickerFileUpdateStatus(
                  clickHouseDAO, ent.getValue(), TickerFile.FileStatus.ERROR);
            } catch (Exception e) {
              LOGGER.error("UPLOADING TICKERS DATA FUTURES EXCEPTION - ", e);
            }
          }

          DecimalFormat df = new DecimalFormat("0.00");
          double totalUploadingTime =
              (double) (System.currentTimeMillis() - uploadingStartTime) / 1000;
          String totalUploadingTimeStr = df.format(totalUploadingTime);

          LOGGER.info(
              "Finished uploading {} tickers files data: {} sec.",
              tickerFiles.size(),
              totalUploadingTimeStr);
          return RETRIEVE_PREPARED_FILES.setDelay(Duration.ofSeconds(WORK_CYCLE_TIME_SEC));
        },
        MoreExecutors.directExecutor());
  }
}
