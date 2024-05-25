package com.crypto.service.util;

import com.clickhouse.client.ClickHouseException;
import com.crypto.service.dao.ClickHouseDAO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;


import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class TickersDataReaderTest {

  @Mock ClickHouseDAO clickHouseDAO;
  @Mock TickersDataReader tickersDataReader;

  @Test
  public void whenExceptionThrownFromClickHouseDAOCompressionHandlerCreatedAnotherInstance()
      throws ClickHouseException {

    doThrow(new RuntimeException("TEST_EXCEPTION #1"))
        .when(clickHouseDAO)
        .insertFromCompressedFileStream(any(), anyString());

    Thread insertThread = new Thread(() -> tickersDataReader.readExecutor(), "INSERT-THREAD");
    insertThread.start();

    try (MockedStatic<CompressionHandler> mockedCompression =
        Mockito.mockStatic(CompressionHandler.class)) {
      mockedCompression.verify(
          () -> CompressionHandler.createCompressionHandler(any(), any(), any()), times(2));
    }
  }
}
