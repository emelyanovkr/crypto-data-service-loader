package com.crypto.service.flow;

import com.clickhouse.client.ClickHouseException;
import com.crypto.service.MainApplication;
import com.crypto.service.config.MainFlowsConfig;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.dao.Tables;
import com.crypto.service.data.TickerFile;
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

// FLOW 4
@FlowType(firstStep = "PREPARE_TO_CLEAN_FILES")
public class CleanupUploadedFilesFlow {
  protected static final Logger LOGGER = LoggerFactory.getLogger(CleanupUploadedFilesFlow.class);
  protected final MainFlowsConfig mainFlowsConfig;
  protected static int WORK_CYCLE_TIME_HOURS;
  protected static final String CREATE_DATE_COLUMN = "create_date";

  @State protected ClickHouseDAO clickHouseDAO;
  @State protected final String rootPath;
  @State protected LocalDate firstDateOfUploadedFile;
  @State protected LocalDate lastDateOfUploadedFile;

  public CleanupUploadedFilesFlow(String rootPath) {
    mainFlowsConfig = MainApplication.mainFlowsConfig;
    WORK_CYCLE_TIME_HOURS = mainFlowsConfig.getCleanupUploadedFilesConfig().getWorkCycleTimeHours();

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
    LocalDate firstFileUploadDateAcquired;
    LocalDate lastFileUploadDateAcquired;
    try {
      firstFileUploadDateAcquired =
          clickHouseDAO.selectFinishedTickerFilesDate(
              "MIN",
              CREATE_DATE_COLUMN,
              Tables.TICKER_FILES.getTableName(),
              TickerFile.FileStatus.FINISHED);
      lastFileUploadDateAcquired =
          clickHouseDAO.selectFinishedTickerFilesDate(
              "MAX",
              CREATE_DATE_COLUMN,
              Tables.TICKER_FILES.getTableName(),
              TickerFile.FileStatus.FINISHED);

    } catch (Exception e) {
      LOGGER.error("ERROR ACQUIRING MIN TICKER FILES DATE - ", e);
      throw new RuntimeException(e);
    }

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

    // TODO: reconsider
    //  1. Выкачать всю пачку файлов и проверять их статус локально
    //  2. Делать лукап прямо в базу данных по имени файла и проверять статус
    //  3. Выкачать только пачку файлов со статусом ERROR и проверять статус локально

    Path rootDirectory = Paths.get(rootPath);
    try (DirectoryStream<Path> stream =
        Files.newDirectoryStream(rootDirectory, Files::isDirectory)) {

      int deletedFilesCounter = 0;
      int deletedDirsCounter = 0;
      int leftFilesCounter = 0;

      List<String> deletedDirectories = new ArrayList<>();

      for (Path entry : stream) {
        LocalDate directoryDate = LocalDate.parse(entry.getFileName().toString());
        if (directoryDate.plusDays(1).isBefore(lastDateOfUploadedFile)) {
          try (DirectoryStream<Path> fileStream =
              Files.newDirectoryStream(entry, Files::isRegularFile)) {
            for (Path file : fileStream) {
              String requestedStatus =
                  clickHouseDAO.selectFileStatusOnFilename(
                      Tables.TICKER_FILES.getTableName(), file.getFileName().toString());
              TickerFile.FileStatus parsedFileStatus =
                  TickerFile.FileStatus.valueOf(requestedStatus);
              if (parsedFileStatus == TickerFile.FileStatus.FINISHED) {
                deletedFilesCounter++;
                Files.delete(file);
              } else if (parsedFileStatus == TickerFile.FileStatus.ERROR) {
                leftFilesCounter++;
              }
            }
          } catch (ClickHouseException e) {
            LOGGER.error("ERROR SELECTING FILE STATUS FOR DIRECTORY {} - ", entry, e);
          }
          if (leftFilesCounter == 0) {
            deletedDirectories.add(entry.getFileName().toString());
            Files.delete(entry);
            deletedDirsCounter++;
            LOGGER.info("Successfully deleted directory: {}", entry);
          }
        }
      }
      if (deletedFilesCounter > 0 || leftFilesCounter != 0) {
        LOGGER.info(
            "Cleanup files finished: {} dirs deleted, {} files deleted, {} files left (ERROR status)",
            deletedDirsCounter,
            deletedFilesCounter,
            leftFilesCounter);
        LOGGER.info("Deleted dirs: {}", deletedDirectories);
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
