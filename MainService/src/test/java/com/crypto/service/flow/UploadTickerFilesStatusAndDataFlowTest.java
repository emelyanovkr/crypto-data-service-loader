package com.crypto.service.flow;

import com.clickhouse.client.ClickHouseException;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.dao.Tables;
import com.crypto.service.data.TickerFile;
import com.crypto.service.util.WorkersUtil;
import com.flower.conf.Transition;
import com.flower.engine.function.FlowerOutPrm;
import com.google.common.io.Resources;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.Month;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class UploadTickerFilesStatusAndDataFlowTest {

  private static final String TEST_DATA_PATH;
  private static final String TEST_FILE_A = "0000A";
  private static final String TEST_FILE_C = "0000C";
  private static final String TEST_FILE_G = "0000G";
  private static final String TEST_FILE_I = "0000I";

  static {
    try {
      TEST_DATA_PATH = Paths.get(Resources.getResource("TestData").toURI()).toString();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Mock ClickHouseDAO clickHouseDAO;
  @Mock Transition UPLOAD_TICKERS_FILES_DATA;
  @Mock Transition FILL_PATHS_LIST;
  @Mock Transition RETRIEVE_PREPARED_FILES;

  // Checking that clickhouseDAO called with correct parameters
  @Test
  public void clickhouseDaoCalledWithRightParametersForUpdatingFileStatuses() {
    List<TickerFile> testData =
        List.of(
            new TickerFile(
                TEST_FILE_A, LocalDate.now(), TickerFile.FileStatus.READY_FOR_PROCESSING),
            new TickerFile(
                TEST_FILE_C, LocalDate.now(), TickerFile.FileStatus.READY_FOR_PROCESSING));
    try {
      when(clickHouseDAO.selectTickerFilesNamesOnStatus(anyString(), any())).thenReturn(testData);
      FlowerOutPrm<List<TickerFile>> tickerFiles = new FlowerOutPrm<>();

      UploadTickerFilesStatusAndDataFlow.RETRIEVE_PREPARED_FILES(
          tickerFiles, clickHouseDAO, FILL_PATHS_LIST);

      verify(clickHouseDAO)
          .updateTickerFilesStatus(
              TickerFile.getSQLFileNames(testData),
              TickerFile.FileStatus.IN_PROGRESS,
              Tables.TICKER_FILES.getTableName());

    } catch (ClickHouseException e) {
      throw new RuntimeException(e);
    }
  }

  // FILL_PATHS_LIST should correctly resolve paths for all files
  @Test
  public void fillPathsListReturnsCorrectPaths() {
    List<TickerFile> testFiles =
        List.of(
            new TickerFile(TEST_FILE_G, LocalDate.of(2024, Month.AUGUST, 6), null),
            new TickerFile(TEST_FILE_A, LocalDate.of(2024, Month.AUGUST, 8), null),
            new TickerFile(TEST_FILE_C, LocalDate.of(2024, Month.AUGUST, 8), null),
            new TickerFile(TEST_FILE_I, LocalDate.of(2024, Month.AUGUST, 10), null));
    FlowerOutPrm<List<Path>> filePaths = new FlowerOutPrm<>();

    Path TEST_DIR_SIX = Paths.get(TEST_DATA_PATH, LocalDate.of(2024, Month.AUGUST, 6).toString());
    Path TEST_DIR_TEN = Paths.get(TEST_DATA_PATH, LocalDate.of(2024, Month.AUGUST, 10).toString());
    try {
      Files.createDirectory(TEST_DIR_SIX);
      Files.createDirectory(TEST_DIR_TEN);
      Files.createFile(Paths.get(TEST_DIR_SIX.toString(), TEST_FILE_G));
      Files.createFile(Paths.get(TEST_DIR_TEN.toString(), TEST_FILE_I));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    UploadTickerFilesStatusAndDataFlow.FILL_PATHS_LIST(
        testFiles, filePaths, TEST_DATA_PATH, UPLOAD_TICKERS_FILES_DATA);

    Path basePath = Paths.get(TEST_DATA_PATH);
    List<String> resultPaths =
        filePaths.getOpt().get().stream()
            .map(path -> basePath.relativize(Paths.get(path.toUri())).toString())
            .toList();

    List<String> expectedData =
        List.of(
            testFiles.get(0).getCreateDate() + "\\" + testFiles.get(0).getFileName(),
            testFiles.get(1).getCreateDate() + "\\" + testFiles.get(1).getFileName(),
            testFiles.get(2).getCreateDate() + "\\" + testFiles.get(2).getFileName(),
            testFiles.get(3).getCreateDate() + "\\" + testFiles.get(3).getFileName());

    assertEquals(expectedData, resultPaths);

    try {
      Files.delete(Paths.get(TEST_DIR_SIX.toString(), TEST_FILE_G));
      Files.delete(Paths.get(TEST_DIR_TEN.toString(), TEST_FILE_I));
      Files.delete(TEST_DIR_SIX);
      Files.delete(TEST_DIR_TEN);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // Empty list sent to a step, so no work must be done, and it should return to the start
  @Test
  public void uploadTickerFilesDataShouldReturnWhenTickerFilesIsEmpty() {
    UploadTickerFilesStatusAndDataFlow.UPLOAD_TICKERS_FILES_DATA(
        clickHouseDAO, new ArrayList<>(), null, RETRIEVE_PREPARED_FILES);

    verify(RETRIEVE_PREPARED_FILES).setDelay(any());
    verifyNoInteractions(clickHouseDAO);
  }

  @Test
  public void handleUpdateStatusShouldCalledChangeTickerFileStatusMethodTwice() {
    List<TickerFile> tickerFiles =
        List.of(new TickerFile(TEST_FILE_A, LocalDate.now(), TickerFile.FileStatus.FINISHED));
    SettableFuture<Void> future = SettableFuture.create();
    Map<ListenableFuture<Void>, List<TickerFile>> map =
        Map.of(future, tickerFiles);

    future.set(null);

    try (MockedStatic<WorkersUtil> mockedWorkersUtil = mockStatic(WorkersUtil.class)) {
      UploadTickerFilesStatusAndDataFlow.handleUpdateStatus(clickHouseDAO, map, tickerFiles, System.currentTimeMillis());
      mockedWorkersUtil.verify(
          () -> WorkersUtil.changeTickerFileUpdateStatus(any(), anyList(), any()),
          Mockito.times(2));
    }
  }
}
