package com.crypto.service.flow;

import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.crypto.service.MainApplication;
import com.crypto.service.config.ApplicationConfig;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.dao.Tables;
import com.crypto.service.data.TickerFile;
import com.crypto.service.util.ConnectionHandler;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.flower.conf.Transition;
import com.flower.engine.function.FlowerInOutPrm;
import com.flower.engine.function.FlowerOutPrm;
import com.google.common.io.Resources;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.time.LocalDate;
import java.time.Month;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@Execution(ExecutionMode.SAME_THREAD)
public class SaveNewFilesToDbFlowTest {

  private static final String TEST_FILE_A = "0000A";
  private static final String TEST_FILE_B = "0000B";
  private static final String TEST_FILE_C = "0000C";
  private static final String TEST_FILE_D = "0000D";
  private static final String TEST_FILE_X = "0000X";

  @Mock private ClickHouseDAO clickHouseDAO;
  @Mock private static ClickHouseNode clickHouseNode;
  @Mock Transition INIT_DIRECTORY_WATCHER_SERVICE;
  @Mock Transition GET_DIRECTORY_WATCHER_EVENTS_AND_ADD_TO_BUFFER;
  @Mock Transition TRY_TO_FLUSH_BUFFER;
  @Mock Transition POST_FLUSH;

  private static LocalDate TEST_DATE;
  private static String TEST_DATA_PATH;

  @BeforeAll
  public static void setUp() throws URISyntaxException {
    try (MockedStatic<ConnectionHandler> mockedConnection =
        Mockito.mockStatic(ConnectionHandler.class)) {
      mockedConnection.when(ConnectionHandler::initClickHouseConnection).thenReturn(clickHouseNode);

      SaveNewFilesToDbFlow.DISCOVERY_FILES_TIMEOUT_SEC = 5;
      SaveNewFilesToDbFlow.MAX_RECONNECT_ATTEMPTS = 3;
      SaveNewFilesToDbFlow.SLEEP_ON_RECONNECT_MS = 500;

      TEST_DATE = LocalDate.of(2024, Month.AUGUST, 8);
      TEST_DATA_PATH = Paths.get(Resources.getResource("TestData").toURI()).toString();
    }
  }

  // RETRIEVE_FILE_NAMES_LIST_ON_START should correctly collect all files in specified directory
  @Test
  public void retrievedFoldersWithDateMatchedSpecifiedFilesList() {
    List<String> testFiles = List.of(TEST_FILE_A, TEST_FILE_B, TEST_FILE_C, TEST_FILE_D);
    try {
      FlowerOutPrm<Queue<TickerFile>> outFilesBuffer = new FlowerOutPrm<>();
      Queue<TickerFile> outTickerFiles = new LinkedList<>();

      when(clickHouseDAO.selectMaxTickerFilesDate(anyString(), anyString())).thenReturn(TEST_DATE);

      SaveNewFilesToDbFlow.RETRIEVE_FILE_NAMES_LIST_ON_START(
          clickHouseDAO, TEST_DATA_PATH, outFilesBuffer, INIT_DIRECTORY_WATCHER_SERVICE);

      if (outFilesBuffer.getOpt().isPresent()) {
        outTickerFiles = outFilesBuffer.getOpt().get();
      }

      List<String> proceededData = outTickerFiles.stream().map(TickerFile::getFileName).toList();

      assertEquals(testFiles, proceededData);

    } catch (ClickHouseException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void filesBufferIsEmptyShouldBeReturnedToPostFlushStep() {
    FlowerInOutPrm<Long> lastFlushTime = new FlowerInOutPrm<>(5L);
    FlowerInOutPrm<Queue<TickerFile>> localFilesBuffer = new FlowerInOutPrm<>(new LinkedList<>());
    try {
      SaveNewFilesToDbFlow.TRY_TO_FLUSH_BUFFER(
          clickHouseDAO, lastFlushTime, localFilesBuffer, POST_FLUSH);

      // because localFilesBuffer is empty we should return to POST_FLUSH flow
      // that means no interactions with clickhouseDAO
      verifyNoInteractions(clickHouseDAO);

      localFilesBuffer =
          new FlowerInOutPrm<>(
              new LinkedList<>(List.of(new TickerFile(TEST_FILE_A, LocalDate.now(), null))));
      SaveNewFilesToDbFlow.TRY_TO_FLUSH_BUFFER(
          clickHouseDAO, lastFlushTime, localFilesBuffer, POST_FLUSH);

      // now localFilesBuffer has one element at least
      // -> we shouldn't return to POST_FLUSH without interacting with clickhouseDAO
      verify(clickHouseDAO).selectExclusiveTickerFilesNames(anyString(), anyString());
    } catch (ClickHouseException e) {
      throw new RuntimeException(e);
    }
  }

  // GET_DIRECTORY_WATCHER_EVENTS_AND_ADD_TO_BUFFER should add a file to filesBuffer by causing an
  // event from WatcherService
  @Test
  public void createAnEventForWatcherServiceAndFileBeenAddedToFilesBuffer() throws IOException {
    WatchService watchServiceInstance = FileSystems.getDefault().newWatchService();
    FlowerInOutPrm<WatchService> watchService = new FlowerInOutPrm<>(watchServiceInstance);
    String TEST_FILE_NAME = TEST_FILE_X;
    try (MockedStatic<LocalDate> mockedLocalDate = Mockito.mockStatic(LocalDate.class)) {
      mockedLocalDate.when(LocalDate::now).thenReturn(TEST_DATE);

      SaveNewFilesToDbFlow.INIT_DIRECTORY_WATCHER_SERVICE(
          TEST_DATA_PATH,
          new FlowerInOutPrm<>(TEST_DATE),
          watchService,
          INIT_DIRECTORY_WATCHER_SERVICE,
          GET_DIRECTORY_WATCHER_EVENTS_AND_ADD_TO_BUFFER);

      Queue<TickerFile> filesBuffer = new ArrayDeque<>();

      Thread watcherThread =
          new Thread(
              () -> {
                try {
                  for (int i = 0; i < 2; ++i) {
                    Thread.sleep(500);
                    SaveNewFilesToDbFlow.GET_DIRECTORY_WATCHER_EVENTS_AND_ADD_TO_BUFFER(
                        watchService.getOpt().get(), filesBuffer, TRY_TO_FLUSH_BUFFER);
                    Thread.sleep(500);
                  }

                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              },
              "TEST-WATCHER-THREAD-1");

      watcherThread.start();

      Path testDirectoryPath = Paths.get(TEST_DATA_PATH, TEST_DATE.toString());
      Path testFilePath = testDirectoryPath.resolve(TEST_FILE_NAME);

      if (Files.exists(testFilePath)) {
        Files.delete(testFilePath);
      }

      Files.createFile(testFilePath);

      Thread.sleep(1000);

      assertEquals(filesBuffer.poll().getFileName(), TEST_FILE_NAME);

      Files.delete(testFilePath);

    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void todayDateIsNotExistExceptionShouldBeThrown() throws IOException {
    WatchService watchServiceInstance = FileSystems.getDefault().newWatchService();
    FlowerInOutPrm<WatchService> watchService = new FlowerInOutPrm<>(watchServiceInstance);
    String notExistentDir = Paths.get(TEST_DATA_PATH, "NO_DIR").toString();
    assertThrows(
        RuntimeException.class,
        () ->
            SaveNewFilesToDbFlow.INIT_DIRECTORY_WATCHER_SERVICE(
                notExistentDir,
                new FlowerInOutPrm<>(TEST_DATE),
                watchService,
                INIT_DIRECTORY_WATCHER_SERVICE,
                GET_DIRECTORY_WATCHER_EVENTS_AND_ADD_TO_BUFFER));
  }

  @Test
  public void todayDateIsNotExistLastDateShouldBeChosen() throws IOException {
    WatchService watchServiceInstance = FileSystems.getDefault().newWatchService();
    FlowerInOutPrm<WatchService> watchService = new FlowerInOutPrm<>(watchServiceInstance);
    FlowerInOutPrm<LocalDate> CHOSEN_DATE = Mockito.mock(FlowerInOutPrm.class);
    SaveNewFilesToDbFlow.INIT_DIRECTORY_WATCHER_SERVICE(
        TEST_DATA_PATH,
        CHOSEN_DATE,
        watchService,
        INIT_DIRECTORY_WATCHER_SERVICE,
        GET_DIRECTORY_WATCHER_EVENTS_AND_ADD_TO_BUFFER);
    verify(CHOSEN_DATE).setOutValue(TEST_DATE);
  }

  // TRY_TO_FLUSH_BUFFER should send to database only files that are not loaded into the database
  @Test
  public void localExclusiveDataCorrectlySentToDatabase() {
    FlowerInOutPrm<Long> lastFlushTime = new FlowerInOutPrm<>(5L);

    FlowerInOutPrm<Queue<TickerFile>> localFilesBuffer =
        new FlowerInOutPrm<>(
            new LinkedList<>(
                Arrays.asList(
                    new TickerFile(TEST_FILE_A, TEST_DATE, null),
                    new TickerFile(TEST_FILE_B, TEST_DATE, null),
                    new TickerFile(TEST_FILE_C, TEST_DATE, null))));

    try {
      List<String> testFilesFromDatabase = List.of(TEST_FILE_C, TEST_FILE_X);
      when(clickHouseDAO.selectExclusiveTickerFilesNames(anyString(), anyString()))
          .thenReturn(testFilesFromDatabase);

      SaveNewFilesToDbFlow.TRY_TO_FLUSH_BUFFER(
          clickHouseDAO, lastFlushTime, localFilesBuffer, POST_FLUSH);

      Queue<TickerFile> toCheckWithLocalFiles =
          new LinkedList<>(
              List.of(
                  new TickerFile(TEST_FILE_A, TEST_DATE, TickerFile.FileStatus.DISCOVERED),
                  new TickerFile(TEST_FILE_B, TEST_DATE, TickerFile.FileStatus.DISCOVERED)));

      assertEquals(toCheckWithLocalFiles, localFilesBuffer.getInValue());
      verify(clickHouseDAO, atLeastOnce())
          .insertTickerFilesInfo(
              TickerFile.formDataToInsert(localFilesBuffer.getInValue()),
              Tables.TICKER_FILES.getTableName());

    } catch (ClickHouseException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void dateChangedShouldCallWatcherReInit() {
    SaveNewFilesToDbFlow.POST_FLUSH(
        LocalDate.now().minusDays(1), INIT_DIRECTORY_WATCHER_SERVICE, POST_FLUSH);
    verify(INIT_DIRECTORY_WATCHER_SERVICE, atLeastOnce()).setDelay(any());
    verifyNoInteractions(POST_FLUSH);
  }
}
