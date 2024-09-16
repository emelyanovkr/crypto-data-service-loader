package com.crypto.service.util;

import com.clickhouse.client.ClickHouseException;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.dao.Tables;
import com.crypto.service.data.TickerFile;
import org.slf4j.Logger;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class FlowsUtil {
  public static void changeTickerFileUpdateStatus(
      ClickHouseDAO clickHouseDAO, List<TickerFile> tickerFiles, TickerFile.FileStatus status)
      throws ClickHouseException {
    clickHouseDAO.updateTickerFilesStatus(
        TickerFile.getSQLFileNames(
            tickerFiles.stream()
                .filter(tickerFile -> tickerFile.getStatus() == status)
                .collect(Collectors.toList())),
        status,
        Tables.TICKER_FILES.getTableName());
  }

  public static <T> T manageRetryOperation(
      int SLEEP_ON_RECONNECT_MS,
      int MAX_RECONNECT_ATTEMPTS,
      Logger LOGGER,
      String errorMsg,
      Callable<T> operation) {
    for (int currentAttempt = 0; currentAttempt < MAX_RECONNECT_ATTEMPTS; currentAttempt++) {
      try {
        return operation.call();
      } catch (Exception e) {
        LOGGER.error(
            "{}, RETRYING #{} IN {} MS - ", errorMsg, currentAttempt, SLEEP_ON_RECONNECT_MS, e);

        if (currentAttempt < MAX_RECONNECT_ATTEMPTS - 1) {
          try {
            Thread.sleep(SLEEP_ON_RECONNECT_MS);
          } catch (InterruptedException ie) {
            throw new RuntimeException("INTERRUPTING RETRYING THREAD: " + ie);
          }
        }
      }
    }
    LOGGER.error("UNSUCCESSFUL RECONNECTING WITH {} TRIES, TERMINATING.", MAX_RECONNECT_ATTEMPTS);
    throw new RuntimeException("UNSUCCESSFUL RECONNECT");
  }
}
