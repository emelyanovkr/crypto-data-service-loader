package com.crypto.service.data;

import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.crypto.service.MainApplication;
import com.crypto.service.config.ApplicationConfig;
import com.crypto.service.config.TickersDataConfig;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.util.CompressionHandler;
import com.crypto.service.util.ConnectionHandler;
import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.io.Resources;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class TickersDataLoaderTest {
  @Mock private ClickHouseDAO clickHouseDAO;
  @Mock private ClickHouseNode clickHouseNode;

  private static final String CONFIG_NAME = "application.yaml";

  private TickersDataLoader spyTickers;
  private TickersDataLoader.TickersInsertTask spyInsertTask;

  List<TickerFile> TEST_DATA =
      List.of(
          new TickerFile("TEST_ONE", LocalDate.now(), TickerFile.FileStatus.DISCOVERED),
          new TickerFile("TEST_TWO", LocalDate.now(), TickerFile.FileStatus.DISCOVERED));
  List<Path> TEST_DATA_PATH =
      new ArrayList<>(List.of(Path.of("/path/TEST_ONE"), Path.of("/path/TEST_TWO")));

  @BeforeEach
  public void setUp() throws ClickHouseException {

    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    try (MockedStatic<ConnectionHandler> mockedConnection =
        Mockito.mockStatic(ConnectionHandler.class)) {
      mockedConnection.when(ConnectionHandler::initClickHouseConnection).thenReturn(clickHouseNode);

      ApplicationConfig applicationConfig =
          mapper.readValue(Resources.getResource(CONFIG_NAME), ApplicationConfig.class);
      TickersDataConfig tickersDataConfig = applicationConfig.getTickersDataConfig();

      MainApplication.applicationConfig = applicationConfig;
      MainApplication.tickersDataConfig = tickersDataConfig;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    prepareUploadTickersDataForTesting();
  }

  public void prepareUploadTickersDataForTesting() throws ClickHouseException {
    TickersDataLoader tickersDataLoader = new TickersDataLoader(TEST_DATA_PATH, TEST_DATA);
    spyTickers = spy(tickersDataLoader);

    SettableFuture<Void> future = SettableFuture.create();

    TickersDataLoader.TickersInsertTask tickersInsertTask =
        spyTickers.new TickersInsertTask(TEST_DATA_PATH, TEST_DATA, future);
    spyInsertTask = spy(tickersInsertTask);

    spyTickers.clickHouseDAO = clickHouseDAO;

    doThrow(new RuntimeException("TEST_EXCEPTION #1"))
        .when(spyTickers.clickHouseDAO)
        .insertTickersData(any(), anyString());
  }

  @Test
  public void whenExceptionThrownFromClickHouseDAOCompressionHandlerCreatedAnotherInstance() {
    try (MockedStatic<CompressionHandler> mockCompression =
        Mockito.mockStatic(CompressionHandler.class)) {
      mockCompression
          .when(() -> CompressionHandler.createCompressionHandler(any(), any(), any()))
          .thenCallRealMethod();

      spyInsertTask.startInsertTickers();

      mockCompression.verify(
          () -> CompressionHandler.createCompressionHandler(any(), any(), any()),
          times(spyTickers.MAX_FLUSH_DATA_ATTEMPTS));
    }
  }

  @Test
  public void clickHouseLogDaoThrowsExceptionAndNoRestartOfCompressing()
      throws ClickHouseException {
    spyInsertTask.startInsertTickers();

    // should be called only 3 times, because MAX_FLUSH_ATTEMPTS = 3 IN .CONFIG FILE
    verify(spyTickers.clickHouseDAO, times(spyTickers.MAX_FLUSH_DATA_ATTEMPTS))
        .insertTickersData(any(), anyString());
  }
}
