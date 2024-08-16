package com.crypto.service.flow;

import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.crypto.service.MainApplication;
import com.crypto.service.config.ApplicationConfig;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.data.TickerFile;
import com.crypto.service.util.ConnectionHandler;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.flower.conf.OutPrm;
import com.flower.conf.Transition;
import com.google.common.io.Resources;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.Month;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class SaveNewFilesToDbFlowTest {

  private static final String CONFIG_NAME;

  @Mock private ClickHouseDAO clickHouseDAO;
  @Mock private ClickHouseNode clickHouseNode;
  @Spy OutPrm<Queue<TickerFile>> filesBuffer;
  @Mock Transition INIT_DIRECTORY_WATCHER_SERVICE;

  private static final LocalDate TEST_DATE;
  private static final String TEST_DATA_PATH;

  static {
    try {
      CONFIG_NAME = "application.yaml";
      TEST_DATE = LocalDate.of(2024, Month.AUGUST, 8);
      TEST_DATA_PATH = Paths.get(Resources.getResource("TestData").toURI()).toString();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  // TODO: consider to move in test_util package duplicate code
  @BeforeEach
  public void setUp() {

    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    try (MockedStatic<ConnectionHandler> mockedConnection =
        Mockito.mockStatic(ConnectionHandler.class)) {
      mockedConnection.when(ConnectionHandler::initClickHouseConnection).thenReturn(clickHouseNode);

      MainApplication.applicationConfig =
          mapper.readValue(Resources.getResource(CONFIG_NAME), ApplicationConfig.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // Testing RETRIEVE_FILE_NAMES_LIST_ON_START step
  @Test
  public void retrievedFoldersWithDateMatchedSpecifiedFilesList() {
    List<String> testFiles = List.of("0000A", "0000B", "0000C", "0000D", "0000E", "0000F");
    try (MockedStatic<SaveNewFilesToDbFlow> mockedFlow =
        Mockito.mockStatic(SaveNewFilesToDbFlow.class)) {

      List<String> proceededData = new ArrayList<>();

      doAnswer(
              invocation -> {
                Queue<TickerFile> toSetQueue = invocation.getArgument(0);
                proceededData.addAll(toSetQueue.stream().map(TickerFile::getFileName).toList());
                return null;
              })
          .when(filesBuffer)
          .setOutValue(any());

      mockedFlow
          .when(
              () ->
                  SaveNewFilesToDbFlow.RETRIEVE_FILE_NAMES_LIST_ON_START(
                      any(), anyString(), any(), any()))
          .thenCallRealMethod();
      when(clickHouseDAO.selectMaxTickerFilesDate(anyString(), anyString())).thenReturn(TEST_DATE);

      SaveNewFilesToDbFlow.RETRIEVE_FILE_NAMES_LIST_ON_START(
          clickHouseDAO, TEST_DATA_PATH, filesBuffer, INIT_DIRECTORY_WATCHER_SERVICE);

      assertEquals(testFiles, proceededData);

    } catch (ClickHouseException e) {
      throw new RuntimeException(e);
    }
  }

  // Testing GET_DIRECTORY_WATCHER_EVENTS_AND_ADD_TO_BUFFER step
  @Test
  public void createAnEventForWatcherServiceAndFileBeenAddedToFilesBuffer() {}

  // Testing TRY_TO_FLUSH_BUFFER step
  @Test
  public void insertTickersFilesInfoCalledTwiceWithinTimeout() {}
}
