package com.crypto.service.flow;

import com.clickhouse.client.ClickHouseException;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.dao.Tables;
import com.crypto.service.data.TickerFile;
import com.flower.conf.Transition;
import com.flower.engine.function.FlowerOutPrm;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.time.Month;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class CleanupUploadedFilesFlowTest {

  @Mock ClickHouseDAO clickHouseDAO;
  @Mock Transition PREPARE_TO_CLEAN_FILES;
  @Mock Transition CLEANUP_FILES;

  // Testing PREPARE_TO_CLEAN_FILES step
  @Test
  public void bothDatesAreEqualAndShouldReturnToPreparingStep() throws ClickHouseException {

    // 1. FIRST date == LAST date of uploaded file

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

  @Test
  public void firstDateIsEqualTodayAndShouldReturnToPreparingStep() throws ClickHouseException {

    // 2. FIRST date of uploaded file == Today

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
  @Test
  public void firstDateIsEqualYesterdayAndShouldReturnToPreparingStep() throws ClickHouseException {

    // 3. FIRST date of uploaded file == Yesterday

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
}
