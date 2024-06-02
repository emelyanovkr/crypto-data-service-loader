package com.clickhouse.appender.manager;

import com.clickhouse.appender.dao.ClickHouseLogDAO;
import com.clickhouse.appender.util.ConnectionSettings;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class LogBufferManagerTests {
  LogBufferManager spyManager;

  @Mock ConnectionSettings connectionSettings;
  @Mock ClickHouseNode node;
  @Mock ClickHouseLogDAO clickHouseLogDAO;

  @BeforeEach
  void setUp() throws IOException {
    when(connectionSettings.initClickHouseConnection()).thenReturn(node);
    when(node.getProtocol()).thenReturn(ClickHouseProtocol.HTTP);
    LogBufferManager logBufferManager =
        new LogBufferManager(8192, 30, "test", 2, 0, connectionSettings);
    spyManager = spy(logBufferManager);

    spyManager.clickHouseLogDAO = clickHouseLogDAO;
  }

  @Test
  public void flushCalledWithTrueConditions() throws InterruptedException {
    doReturn(true).when(spyManager).flushRequired(anyInt(), anyLong());

    Thread bufferManagement = new Thread(() -> spyManager.bufferManagement());
    bufferManagement.start();

    Thread.sleep(500);

    verify(spyManager, timeout(5000).atLeastOnce()).flush();
  }

  @Test
  public void flushRequiredCalledExactlyTenTimes() {
    spyManager.clickHouseLogDAO = clickHouseLogDAO;

    Thread bufferManagement = new Thread(() -> spyManager.bufferManagement());
    bufferManagement.start();

    verify(spyManager, timeout(1100).atLeast(10)).flushRequired(anyInt(), anyLong());
  }

  @Test
  public void oldMessageWasInsertedAndNewOneIsNotWhenFlushGotOldQueue()
      throws InterruptedException, ClickHouseException {
    long TEST_TIMESTAMP = 11111;
    String TEST_MESSAGE = "TEST MESSAGE #1";
    String TEST_MESSAGE_2 = "TEST MESSAGE #2";

    Thread insertThread =
        new Thread(
            () -> {
              spyManager.insertLogMsg(TEST_TIMESTAMP, TEST_MESSAGE);
              spyManager.insertLogMsg(TEST_TIMESTAMP + TEST_TIMESTAMP * 2, TEST_MESSAGE_2);
            },
            "INSERT-THREAD");

    Thread flushThread = new Thread(spyManager::flush, "FLUSH-THREAD");

    doAnswer(
            invocation -> {
              Thread.sleep(500);
              return invocation.callRealMethod();
            })
        .when(spyManager)
        .getAndIncrement(any());

    insertThread.start();
    Thread.sleep(50);
    flushThread.start();

    verify(spyManager)
        .clickHouseLogDAO
        .insertLogData(spyManager.createLogMsg(TEST_TIMESTAMP, TEST_MESSAGE));

    verify(spyManager.clickHouseLogDAO, times(0))
        .insertLogData(
            spyManager.createLogMsg(TEST_TIMESTAMP + TEST_TIMESTAMP * 2, TEST_MESSAGE_2));
  }

  @Test
  public void insertLogDataThrowsLessExceptionThanMaxFlushAttempts() throws ClickHouseException {
    long TEST_TIMESTAMP = 11111;
    String TEST_MESSAGE = "TEST MESSAGE #1";

    doThrow(new RuntimeException("TEST_EXCEPTION #1"))
        .when(spyManager.clickHouseLogDAO)
        .insertLogData(anyString());

    spyManager.insertLogMsg(TEST_TIMESTAMP, TEST_MESSAGE);
    spyManager.flush();

    verify(spyManager.clickHouseLogDAO, timeout(1000).times(2)).insertLogData(anyString());

    assertThrows(
        Exception.class,
        () ->
            clickHouseLogDAO.insertLogData(spyManager.createLogMsg(TEST_TIMESTAMP, TEST_MESSAGE)));
  }

  @Test
  public void flushWillWaitOnSpinningLockWhileAddingToQueueLogMsg()
      throws InterruptedException, ClickHouseException {
    long TEST_TIMESTAMP = 11111;
    String TEST_MESSAGE = "TEST MESSAGE #1";

    doAnswer(
            invocation -> {
              Thread.sleep(500);
              return invocation.callRealMethod();
            })
        .when(spyManager)
        .addToQueue(any(), any());

    Thread insertThread =
        new Thread(() -> spyManager.insertLogMsg(TEST_TIMESTAMP, TEST_MESSAGE), "INSERT-THREAD");
    insertThread.start();

    Thread.sleep(50);

    spyManager.flush();

    verify(spyManager.clickHouseLogDAO, timeout(600).times(1))
        .insertLogData(spyManager.createLogMsg(TEST_TIMESTAMP, TEST_MESSAGE));
  }
}
