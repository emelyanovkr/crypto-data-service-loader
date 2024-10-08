package com.crypto.service.flow;

import com.clickhouse.client.ClickHouseException;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.dao.Tables;
import com.crypto.service.data.TickerFile;
import com.crypto.service.util.FlowsUtil;
import com.flower.conf.Transition;
import com.flower.engine.function.FlowerOutPrm;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import static com.crypto.service.data.TickerFile.FileStatus.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class ProceedFilesStatusFlowTest {

  @Mock private ClickHouseDAO clickHouseDAO;
  @Mock private Transition RETRIEVE_TICKER_FILES_INFO;
  @Mock private Transition PROCEED_FILES_STATUS;

  private static final String TEST_FILE_A = "0000A";
  private static final String TEST_FILE_B = "0000B";

  @BeforeAll
  public static void setUp() {
    ProceedFilesStatusFlow.SLEEP_ON_RECONNECT_MS = 500;
    ProceedFilesStatusFlow.MAX_RECONNECT_ATTEMPTS = 3;
  }

  // In RETRIEVE_TICKER_FILES_INFO clickhouseDAO should be correctly called with right arguments
  @Test
  public void clickhouseDaoCalledWithCorrectArgumentsToRetrieveFilesList()
      throws ClickHouseException {
    FlowerOutPrm<List<TickerFile>> tickerFiles = new FlowerOutPrm<>();
    ProceedFilesStatusFlow.RETRIEVE_TICKER_FILES_INFO(
        clickHouseDAO, tickerFiles, PROCEED_FILES_STATUS);
    verify(clickHouseDAO, times(1))
        .selectTickerFilesNamesOnStatus(
            Tables.TICKER_FILES.getTableName(),
            TickerFile.FileStatus.DISCOVERED,
            TickerFile.FileStatus.DOWNLOADING);
  }

  // RETRIEVE_TICKER_FILES_INFO should change file statuses accordingly
  @Test
  public void fileStatusSetDownloadingForTodayDateFiles() {
    try (MockedStatic<FlowsUtil> mockedFlowsUtil = Mockito.mockStatic(FlowsUtil.class)) {
      List<TickerFile> testTickerFiles =
          new ArrayList<>(
              List.of(
                  new TickerFile(
                      TEST_FILE_A,
                      LocalDate.now(),
                      DISCOVERED), // Status should be set for DOWNLOADING
                  new TickerFile(
                      TEST_FILE_B,
                      LocalDate.now(),
                      TickerFile.FileStatus.ERROR) // Status should be leaved as it is
                  ));

      mockedFlowsUtil
          .when(FlowsUtil.manageRetryOperation(anyInt(), anyInt(), any(), anyString(), any()))
          .thenCallRealMethod();

      ProceedFilesStatusFlow.PROCEED_FILES_STATUS(
          clickHouseDAO, testTickerFiles, RETRIEVE_TICKER_FILES_INFO);

      // Checking that updated method called twice for both files, so they status should be changed
      mockedFlowsUtil.verify(
          () -> FlowsUtil.changeTickerFileUpdateStatus(any(), anyList(), any()), Mockito.times(2));

      // Checking their status change
      assertEquals(testTickerFiles.get(0).getStatus(), DOWNLOADING);
      assertEquals(testTickerFiles.get(1).getStatus(), ERROR);
    }
  }

  // RETRIEVE_TICKER_FILES_INFO should change file statuses accordingly
  @Test
  public void fileStatusSetReadyForProcessingForYesterdayFiles() {
    try (MockedStatic<FlowsUtil> mockedFlowsUtil = Mockito.mockStatic(FlowsUtil.class)) {
      List<TickerFile> testTickerFiles =
          new ArrayList<>(
              List.of(
                  new TickerFile(
                      TEST_FILE_A,
                      LocalDate.now().minusDays(5),
                      DISCOVERED), // Status should be set for READY_FOR_PROCESSING
                  new TickerFile(
                      TEST_FILE_B,
                      LocalDate.now(),
                      TickerFile.FileStatus.ERROR) // Status should be leaved as it is
                  ));

      mockedFlowsUtil
          .when(FlowsUtil.manageRetryOperation(anyInt(), anyInt(), any(), anyString(), any()))
          .thenCallRealMethod();

      ProceedFilesStatusFlow.PROCEED_FILES_STATUS(
          clickHouseDAO, testTickerFiles, RETRIEVE_TICKER_FILES_INFO);

      // Checking that updated method of clickhouseDAO called twice to change status of files
      mockedFlowsUtil.verify(
          () -> FlowsUtil.changeTickerFileUpdateStatus(any(), anyList(), any()), Mockito.times(2));

      // Checking their status change
      assertEquals(testTickerFiles.get(0).getStatus(), READY_FOR_PROCESSING);
      assertEquals(testTickerFiles.get(1).getStatus(), ERROR);
    }
  }
}
