package com.crypto.service.flow;

import com.clickhouse.client.ClickHouseException;
import com.crypto.service.MainApplication;
import com.crypto.service.config.ApplicationConfig;
import com.crypto.service.config.MainFlowsConfig;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.dao.Tables;
import com.crypto.service.data.TickerFile;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.flower.conf.Transition;
import com.flower.engine.function.FlowerOutPrm;
import com.google.common.io.Resources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDate;
import java.time.Month;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@Execution(ExecutionMode.SAME_THREAD)
public class CleanupUploadedFilesFlowTest {

  @Mock ClickHouseDAO clickHouseDAO;
  @Mock Transition PREPARE_TO_CLEAN_FILES;
  @Mock Transition CLEANUP_FILES;

  private static final String CONFIG_NAME = "application.yaml";
  private static final String FINISHED_STATUS = "FINISHED";
  private static final String ERROR_STATUS = "ERROR";
  private static final String TEST_DATA_PATH;
  private static final String TEST_FILE_L = "0000L";
  private static final String TEST_FILE_K = "0000K";

  static {
    try {
      TEST_DATA_PATH = Paths.get(Resources.getResource("TestData").toURI()).toString();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  // 1. FIRST date == LAST date of uploaded file
  @Test
  public void bothDatesAreEqualAndShouldReturnToPreparingStep() throws ClickHouseException {
    LocalDate firstDateTest = LocalDate.of(2024, Month.AUGUST, 24);
    LocalDate lastDateTest = LocalDate.of(2024, Month.AUGUST, 24);

    when(clickHouseDAO.selectFinishedTickerFilesDate(
            CleanupUploadedFilesFlow.MIN_SQL_FUNCTION_NAME,
            CleanupUploadedFilesFlow.CREATE_DATE_COLUMN,
            Tables.TICKER_FILES.getTableName(),
            TickerFile.FileStatus.FINISHED))
        .thenReturn(firstDateTest);

    when(clickHouseDAO.selectFinishedTickerFilesDate(
            CleanupUploadedFilesFlow.MAX_SQL_FUNCTION_NAME,
            CleanupUploadedFilesFlow.CREATE_DATE_COLUMN,
            Tables.TICKER_FILES.getTableName(),
            TickerFile.FileStatus.FINISHED))
        .thenReturn(lastDateTest);

    CleanupUploadedFilesFlow.PREPARE_TO_CLEAN_FILES(
        clickHouseDAO,
        new FlowerOutPrm<>(),
        new FlowerOutPrm<>(),
        PREPARE_TO_CLEAN_FILES,
        CLEANUP_FILES);

    // Return conditions are met, so we should go back from the step
    verify(PREPARE_TO_CLEAN_FILES, times(1)).setDelay(any());

    // And we shouldn't go to the next step -> CLEANUP_FILES
    verifyNoMoreInteractions(CLEANUP_FILES);
  }

  // In this condition we should return to preparing step
  // 2. FIRST date of uploaded file == Today
  @Test
  public void firstDateIsEqualTodayAndShouldReturnToPreparingStep() throws ClickHouseException {
    LocalDate firstDateTest = LocalDate.now();

    when(clickHouseDAO.selectFinishedTickerFilesDate(
            CleanupUploadedFilesFlow.MIN_SQL_FUNCTION_NAME,
            CleanupUploadedFilesFlow.CREATE_DATE_COLUMN,
            Tables.TICKER_FILES.getTableName(),
            TickerFile.FileStatus.FINISHED))
        .thenReturn(firstDateTest);

    when(clickHouseDAO.selectFinishedTickerFilesDate(
            CleanupUploadedFilesFlow.MAX_SQL_FUNCTION_NAME,
            CleanupUploadedFilesFlow.CREATE_DATE_COLUMN,
            Tables.TICKER_FILES.getTableName(),
            TickerFile.FileStatus.FINISHED))
        .thenReturn(firstDateTest.plusDays(1));

    CleanupUploadedFilesFlow.PREPARE_TO_CLEAN_FILES(
        clickHouseDAO,
        new FlowerOutPrm<>(),
        new FlowerOutPrm<>(),
        PREPARE_TO_CLEAN_FILES,
        CLEANUP_FILES);

    // Return conditions are met, so we should go back from the step
    verify(PREPARE_TO_CLEAN_FILES, times(1)).setDelay(any());

    // And we shouldn't go to the next step -> CLEANUP_FILES
    verifyNoMoreInteractions(CLEANUP_FILES);
  }

  // Because firstDate of uploaded file is equals to yesterday we shouldn't delete those files
  // Just to keep them as backup for some time (1 day) on HDD.
  // 3. FIRST date of uploaded file == Yesterday
  @Test
  public void firstDateIsEqualYesterdayAndShouldReturnToPreparingStep() throws ClickHouseException {
    LocalDate firstDateTest = LocalDate.now().minusDays(1);

    when(clickHouseDAO.selectFinishedTickerFilesDate(
            CleanupUploadedFilesFlow.MIN_SQL_FUNCTION_NAME,
            CleanupUploadedFilesFlow.CREATE_DATE_COLUMN,
            Tables.TICKER_FILES.getTableName(),
            TickerFile.FileStatus.FINISHED))
        .thenReturn(firstDateTest);

    when(clickHouseDAO.selectFinishedTickerFilesDate(
            CleanupUploadedFilesFlow.MAX_SQL_FUNCTION_NAME,
            CleanupUploadedFilesFlow.CREATE_DATE_COLUMN,
            Tables.TICKER_FILES.getTableName(),
            TickerFile.FileStatus.FINISHED))
        .thenReturn(firstDateTest.plusDays(1));

    CleanupUploadedFilesFlow.PREPARE_TO_CLEAN_FILES(
        clickHouseDAO,
        new FlowerOutPrm<>(),
        new FlowerOutPrm<>(),
        PREPARE_TO_CLEAN_FILES,
        CLEANUP_FILES);

    // Return conditions are met, so we should go back from the step
    verify(PREPARE_TO_CLEAN_FILES, times(1)).setDelay(any());

    // And we shouldn't go to the next step -> CLEANUP_FILES
    verifyNoMoreInteractions(CLEANUP_FILES);
  }

  // Directory should be cleaned up because all files in directory has status finished
  @Test
  public void directoryShouldBeDeleted() {
    Path TEST_DIR_TODAY_PATH = Paths.get(TEST_DATA_PATH, LocalDate.now().toString());
    Path TEST_FILE_PATH = Paths.get(TEST_DIR_TODAY_PATH.toString(), TEST_FILE_L);

    try {
      Files.createDirectory(TEST_DIR_TODAY_PATH);
      Files.createFile(TEST_FILE_PATH);

      assertTrue(Files.exists(TEST_FILE_PATH));

      when(clickHouseDAO.selectFileStatusOnFilename(anyString(), anyString()))
          .thenReturn(ERROR_STATUS);
      when(clickHouseDAO.selectFileStatusOnFilename(
              Tables.TICKER_FILES.getTableName(), TEST_FILE_L))
          .thenReturn(FINISHED_STATUS);
      CleanupUploadedFilesFlow.CLEANUP_FILES(
          clickHouseDAO, TEST_DATA_PATH, LocalDate.now().plusDays(2), PREPARE_TO_CLEAN_FILES);

      assertFalse(Files.exists(TEST_FILE_PATH));
      assertFalse(Files.exists(TEST_DIR_TODAY_PATH));
    } catch (IOException | ClickHouseException e) {
      throw new RuntimeException(e);
    }
  }

  // One file should be left in directory and not deleted because it's having ERROR status
  @Test
  public void onlyFinishedFilesShouldBeDeleted() {
    Path TEST_DIR_TODAY_PATH = Paths.get(TEST_DATA_PATH, LocalDate.now().toString());
    Path TEST_FILE_L_PATH = Paths.get(TEST_DIR_TODAY_PATH.toString(), TEST_FILE_L);
    Path TEST_FILE_K_PATH = Paths.get(TEST_DIR_TODAY_PATH.toString(), TEST_FILE_K);

    try {
      Files.createDirectory(TEST_DIR_TODAY_PATH);
      Files.createFile(TEST_FILE_L_PATH);
      Files.createFile(TEST_FILE_K_PATH);

      assertTrue(Files.exists(TEST_FILE_L_PATH));
      assertTrue(Files.exists(TEST_FILE_K_PATH));

      when(clickHouseDAO.selectFileStatusOnFilename(anyString(), anyString()))
          .thenReturn("DISCOVERED");
      when(clickHouseDAO.selectFileStatusOnFilename(
              Tables.TICKER_FILES.getTableName(), TEST_FILE_L))
          .thenReturn(FINISHED_STATUS);
      when(clickHouseDAO.selectFileStatusOnFilename(
              Tables.TICKER_FILES.getTableName(), TEST_FILE_K))
          .thenReturn(ERROR_STATUS);

      CleanupUploadedFilesFlow.CLEANUP_FILES(
          clickHouseDAO, TEST_DATA_PATH, LocalDate.now().plusDays(2), PREPARE_TO_CLEAN_FILES);

      assertFalse(Files.exists(TEST_FILE_L_PATH));
      assertTrue(Files.exists(TEST_FILE_K_PATH));

      Files.delete(TEST_FILE_K_PATH);
      Files.delete(TEST_DIR_TODAY_PATH);

    } catch (IOException | ClickHouseException e) {
      throw new RuntimeException(e);
    }
  }

  // All files in directory have status ERROR, so no files should be deleted
  @Test
  public void noFilesShouldBeDeleted() {
    Path TEST_DIR_TODAY_PATH = Paths.get(TEST_DATA_PATH, LocalDate.now().toString());
    Path TEST_FILE_L_PATH = Paths.get(TEST_DIR_TODAY_PATH.toString(), TEST_FILE_L);
    Path TEST_FILE_K_PATH = Paths.get(TEST_DIR_TODAY_PATH.toString(), TEST_FILE_K);

    try {
      Files.createDirectory(TEST_DIR_TODAY_PATH);
      Files.createFile(TEST_FILE_L_PATH);
      Files.createFile(TEST_FILE_K_PATH);

      assertTrue(Files.exists(TEST_FILE_L_PATH));
      assertTrue(Files.exists(TEST_FILE_K_PATH));

      when(clickHouseDAO.selectFileStatusOnFilename(anyString(), anyString()))
          .thenReturn(ERROR_STATUS);

      CleanupUploadedFilesFlow.CLEANUP_FILES(
          clickHouseDAO, TEST_DATA_PATH, LocalDate.now().plusDays(2), PREPARE_TO_CLEAN_FILES);

      assertTrue(Files.exists(TEST_FILE_L_PATH));
      assertTrue(Files.exists(TEST_FILE_K_PATH));

      Files.delete(TEST_FILE_L_PATH);
      Files.delete(TEST_FILE_K_PATH);
      Files.delete(TEST_DIR_TODAY_PATH);

    } catch (IOException | ClickHouseException e) {
      throw new RuntimeException(e);
    }
  }

  // We should retry cleaning in a doubled delay when exception is thrown
  @Test
  public void exceptionThrownAndPreparingCalledWithDelay() {
    String TEST_EXCEPTION_DATA_PATH = Paths.get(TEST_DATA_PATH, "NO_DIR").toString();

    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    MainFlowsConfig mainFlowsConfig;

    try {
      MainApplication.applicationConfig =
          mapper.readValue(Resources.getResource(CONFIG_NAME), ApplicationConfig.class);
      mainFlowsConfig = MainApplication.applicationConfig.getMainFlowsConfig();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    CleanupUploadedFilesFlow flow =
        new CleanupUploadedFilesFlow(mainFlowsConfig, TEST_EXCEPTION_DATA_PATH);

    CleanupUploadedFilesFlow.CLEANUP_FILES(
        clickHouseDAO,
        TEST_EXCEPTION_DATA_PATH,
        LocalDate.now().plusDays(2),
        PREPARE_TO_CLEAN_FILES);

    int WORK_CYCLE_TIME_HOURS =
        mainFlowsConfig.getCleanupUploadedFilesConfig().getWorkCycleTimeHours();
    verify(PREPARE_TO_CLEAN_FILES, times(1)).setDelay(Duration.ofHours(WORK_CYCLE_TIME_HOURS * 2L));
  }
}
