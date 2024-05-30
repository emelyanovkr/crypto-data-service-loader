package com.crypto.service.util;

import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.crypto.service.dao.ClickHouseDAO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class TickersDataReaderTest {
  @Mock ClickHouseDAO clickHouseDAO;
  @Mock ClickHouseNode clickHouseNode;

  TickersDataReader spyTickers;
  TickersDataReader.TickersInsertTask spyInsertTask;

  List<String> TEST_DATA = new ArrayList<>(List.of("TEST_ONE", "TEST_TWO"));

  @BeforeEach
  public void setUp() throws ClickHouseException {

    try (MockedStatic<ConnectionHandler> mockedConnection =
        Mockito.mockStatic(ConnectionHandler.class)) {
      mockedConnection.when(ConnectionHandler::initClickHouseConnection).thenReturn(clickHouseNode);
    }

    prepareReadExecutorForTesting();
  }

  public void prepareReadExecutorForTesting() throws ClickHouseException {
    InitData.setPropertiesField("config_test.properties");
    TickersDataReader tickersDataReader = new TickersDataReader();
    spyTickers = spy(tickersDataReader);

    TickersDataReader.TickersInsertTask tickersInsertTask =
        spyTickers.new TickersInsertTask(TEST_DATA);
    spyInsertTask = spy(tickersInsertTask);

    spyTickers.clickHouseDAO = clickHouseDAO;

    doThrow(new RuntimeException("TEST_EXCEPTION #1"))
        .when(spyTickers.clickHouseDAO)
        .insertFromCompressedFileStream(any(), anyString());
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
          () -> CompressionHandler.createCompressionHandler(any(), any(), any()), times(2));
    }
  }

  @Test
  public void clickHouseLogDaoThrowsExceptionAndNoRestartOfCompressing()
      throws ClickHouseException {
    spyInsertTask.startInsertTickers();

    // should be called only 2 times, because MAX_FLUSH_ATTEMPTS = 2 IN .PROPERTIES FILE
    verify(spyTickers.clickHouseDAO, times(2)).insertFromCompressedFileStream(any(), anyString());
  }
}
