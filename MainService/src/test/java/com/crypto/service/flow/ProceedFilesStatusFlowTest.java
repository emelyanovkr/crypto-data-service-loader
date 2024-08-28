package com.crypto.service.flow;

import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.data.TickerFile;
import com.crypto.service.util.WorkersUtil;
import com.flower.conf.Transition;
import org.junit.jupiter.api.BeforeEach;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;

@ExtendWith(MockitoExtension.class)
public class ProceedFilesStatusFlowTest {

  @Mock private ClickHouseDAO clickHouseDAO;
  @Mock private Transition RETRIEVE_TICKER_FILES_INFO;

  private static final String TEST_FILE_A = "0000A";
  private static final String TEST_FILE_B = "0000B";

  // Testing RETRIEVE_TICKER_FILES_INFO flow
  @Test
  public void fileStatusSetDownloadingForTodayDateFiles() {
    try (MockedStatic<WorkersUtil> mockedWorkersUtil = Mockito.mockStatic(WorkersUtil.class)) {
      List<TickerFile> testTickerFiles =
          new ArrayList<>(
              List.of(
                  new TickerFile(
                    TEST_FILE_A, LocalDate.now(), DISCOVERED), // Status should be set for DOWNLOADING
                  new TickerFile(
                    TEST_FILE_B,
                      LocalDate.now(),
                      TickerFile.FileStatus.ERROR) // Status should be leaved as it is
                  ));

      ProceedFilesStatusFlow.PROCEED_FILES_STATUS(
          clickHouseDAO, testTickerFiles, RETRIEVE_TICKER_FILES_INFO);

      mockedWorkersUtil.verify(
          () -> WorkersUtil.changeTickerFileUpdateStatus(any(), anyList(), any()),
          Mockito.times(2));

      assertEquals(testTickerFiles.get(0).getStatus(), DOWNLOADING);
      assertEquals(testTickerFiles.get(1).getStatus(), ERROR);
    }
  }

  // Testing RETRIEVE_TICKER_FILES_INFO flow
  @Test
  public void fileStatusSetReadyForProcessingForYesterdayFiles() {
    try (MockedStatic<WorkersUtil> mockedWorkersUtil = Mockito.mockStatic(WorkersUtil.class)) {
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

      ProceedFilesStatusFlow.PROCEED_FILES_STATUS(
          clickHouseDAO, testTickerFiles, RETRIEVE_TICKER_FILES_INFO);

      mockedWorkersUtil.verify(
          () -> WorkersUtil.changeTickerFileUpdateStatus(any(), anyList(), any()),
          Mockito.times(2));

      assertEquals(testTickerFiles.get(0).getStatus(), READY_FOR_PROCESSING);
      assertEquals(testTickerFiles.get(1).getStatus(), ERROR);
    }
  }
}
