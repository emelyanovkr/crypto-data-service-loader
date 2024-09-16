package com.crypto.service.flow;

import com.crypto.service.config.MainFlowsConfig;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.dao.Tables;
import com.crypto.service.data.TickerFile;
import com.crypto.service.util.FlowsUtil;
import com.flower.anno.flow.FlowType;
import com.flower.anno.flow.State;
import com.flower.anno.functions.SimpleStepFunction;
import com.flower.anno.params.common.In;
import com.flower.anno.params.common.Out;
import com.flower.anno.params.transit.StepRef;
import com.flower.conf.OutPrm;
import com.flower.conf.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

// FLOW 4
@FlowType(firstStep = "PREPARE_TO_CLEAN_FILES")
public class CleanupUploadedFilesFlow {
  protected static final Logger LOGGER = LoggerFactory.getLogger(CleanupUploadedFilesFlow.class);

  protected final MainFlowsConfig mainFlowsConfig;
  protected static int WORK_CYCLE_TIME_HOURS;
  protected static int SLEEP_ON_RECONNECT_MS;
  protected static int MAX_RECONNECT_ATTEMPTS;
  protected static final String GET_MIN_FILES_DATE_ERROR_MSG =
      "ERROR ACQUIRING MIN TICKER FILES DATE";
  protected static final String GET_TICKER_FILES_ON_STATUS_ERROR_MSG =
      "CAN'T GET TICKER FILES ON STATUS";

  protected static final String CREATE_DATE_COLUMN = "create_date";
  protected static final String MIN_SQL_FUNCTION_NAME = "MIN";
  protected static final String MAX_SQL_FUNCTION_NAME = "MAX";

  @State protected ClickHouseDAO clickHouseDAO;
  @State protected final String rootPath;
  @State protected LocalDate firstDateOfUploadedFile;
  @State protected LocalDate lastDateOfUploadedFile;

  public CleanupUploadedFilesFlow(MainFlowsConfig mainFLowsConfig, String rootPath) {
    this.mainFlowsConfig = mainFLowsConfig;
    WORK_CYCLE_TIME_HOURS = mainFlowsConfig.getCleanupUploadedFilesConfig().getWorkCycleTimeHours();
    SLEEP_ON_RECONNECT_MS = mainFlowsConfig.getCleanupUploadedFilesConfig().getSleepOnReconnectMs();
    MAX_RECONNECT_ATTEMPTS =
        mainFlowsConfig.getCleanupUploadedFilesConfig().getMaxReconnectAttempts();

    this.clickHouseDAO = new ClickHouseDAO();
    this.rootPath = rootPath;
  }

  @SimpleStepFunction
  public static Transition PREPARE_TO_CLEAN_FILES(
      @In(throwIfNull = true) ClickHouseDAO clickHouseDAO,
      @Out OutPrm<LocalDate> firstDateOfUploadedFile,
      @Out OutPrm<LocalDate> lastDateOfUploadedFile,
      @StepRef Transition PREPARE_TO_CLEAN_FILES,
      @StepRef Transition CLEANUP_FILES) {

    LocalDate currentDate = LocalDate.now();
    LocalDate firstFileUploadDateAcquired =
        FlowsUtil.manageRetryOperation(
            SLEEP_ON_RECONNECT_MS,
            MAX_RECONNECT_ATTEMPTS,
            LOGGER,
            GET_MIN_FILES_DATE_ERROR_MSG,
            () ->
                clickHouseDAO.selectFinishedTickerFilesDate(
                    MIN_SQL_FUNCTION_NAME,
                    CREATE_DATE_COLUMN,
                    Tables.TICKER_FILES.getTableName(),
                    TickerFile.FileStatus.FINISHED));

    LocalDate lastFileUploadDateAcquired =
        FlowsUtil.manageRetryOperation(
            SLEEP_ON_RECONNECT_MS,
            MAX_RECONNECT_ATTEMPTS,
            LOGGER,
            GET_MIN_FILES_DATE_ERROR_MSG,
            () ->
                clickHouseDAO.selectFinishedTickerFilesDate(
                    MAX_SQL_FUNCTION_NAME,
                    CREATE_DATE_COLUMN,
                    Tables.TICKER_FILES.getTableName(),
                    TickerFile.FileStatus.FINISHED));

    firstDateOfUploadedFile.setOutValue(firstFileUploadDateAcquired);
    lastDateOfUploadedFile.setOutValue(lastFileUploadDateAcquired);

    // RETURN IF:
    // 1. first date of uploaded file is equals to last date, so files
    //    are loading still in the same day
    // 2. first date of uploaded file is equals to today -> files are still uploading
    // 3. first date of uploaded file is YESTERDAY of current date -> 1 day reserve for backup
    if (firstFileUploadDateAcquired.isEqual(lastFileUploadDateAcquired)
        || firstFileUploadDateAcquired.isEqual(currentDate)
        || firstFileUploadDateAcquired.plusDays(1).isEqual(currentDate)) {
      return PREPARE_TO_CLEAN_FILES.setDelay(Duration.ofHours(WORK_CYCLE_TIME_HOURS * 3L));
    }
    return CLEANUP_FILES;
  }

  @SimpleStepFunction
  public static Transition CLEANUP_FILES(
      @In(throwIfNull = true) ClickHouseDAO clickHouseDAO,
      @In String rootPath,
      @In(throwIfNull = true) LocalDate lastDateOfUploadedFile,
      @StepRef Transition PREPARE_TO_CLEAN_FILES) {

    Path rootDirectory = Paths.get(rootPath);
    try (DirectoryStream<Path> stream =
        Files.newDirectoryStream(rootDirectory, Files::isDirectory)) {

      int deletedFilesCounter = 0;
      int deletedDirsCounter = 0;
      int leftFilesCounter = 0;

      List<String> deletedDirectories = new ArrayList<>();

      for (Path dateDir : stream) {
        LocalDate directoryDate = LocalDate.parse(dateDir.getFileName().toString());
        if (directoryDate.plusDays(1).isBefore(lastDateOfUploadedFile)) {
          try (DirectoryStream<Path> fileStream =
              Files.newDirectoryStream(dateDir, Files::isRegularFile)) {
            for (Path file : fileStream) {
              String requestedStatus =
                  FlowsUtil.manageRetryOperation(
                      SLEEP_ON_RECONNECT_MS,
                      MAX_RECONNECT_ATTEMPTS,
                      LOGGER,
                      GET_TICKER_FILES_ON_STATUS_ERROR_MSG,
                      () ->
                          clickHouseDAO.selectFileStatusOnFilename(
                              Tables.TICKER_FILES.getTableName(), file.getFileName().toString()));

              TickerFile.FileStatus parsedFileStatus =
                  TickerFile.FileStatus.valueOf(requestedStatus);
              if (parsedFileStatus == TickerFile.FileStatus.FINISHED) {
                deletedFilesCounter++;
                Files.delete(file);
              } else if (parsedFileStatus == TickerFile.FileStatus.ERROR) {
                leftFilesCounter++;
              }
            }
            try (Stream<Path> innerDirStream = Files.list(dateDir)) {
              if (!innerDirStream.iterator().hasNext()) {
                deletedDirectories.add(dateDir.getFileName().toString());
                Files.delete(dateDir);
                deletedDirsCounter++;
                LOGGER.info("Successfully deleted directory: {}", dateDir);
              }
            }
          }
        }
      }
      if (deletedFilesCounter > 0 || leftFilesCounter != 0) {
        LOGGER.info(
            "Cleanup files finished: {} dirs deleted, {} files deleted, {} files left (ERROR status)",
            deletedDirsCounter,
            deletedFilesCounter,
            leftFilesCounter);
        if (!deletedDirectories.isEmpty()) {
          LOGGER.info("Deleted dirs: {}", deletedDirectories);
        }
        LOGGER.info("Next cleaning queued in {} hours", WORK_CYCLE_TIME_HOURS);
      } else {
        LOGGER.info("Nothing to clean, retrying in {} hours", WORK_CYCLE_TIME_HOURS);
      }
    } catch (IOException e) {
      LOGGER.error(
          "ERROR ACQUIRING DIRECTORIES, RETRYING IN {} HOURS - ", WORK_CYCLE_TIME_HOURS * 2L, e);
      return PREPARE_TO_CLEAN_FILES.setDelay(Duration.ofHours(WORK_CYCLE_TIME_HOURS * 2L));
    }

    return PREPARE_TO_CLEAN_FILES.setDelay(Duration.ofHours(WORK_CYCLE_TIME_HOURS));
  }
}
