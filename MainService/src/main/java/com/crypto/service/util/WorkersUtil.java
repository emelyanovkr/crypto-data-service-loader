package com.crypto.service.util;

import com.clickhouse.client.ClickHouseException;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.dao.Tables;
import com.crypto.service.data.TickerFile;

import java.util.List;
import java.util.stream.Collectors;

public class WorkersUtil {
  // TODO: improve signature, signature unobvious
  public static void changeTickerFileUpdateStatus(
      ClickHouseDAO clickHouseDAO, List<TickerFile> tickerFiles, TickerFile.FileStatus status) {
    try {
      clickHouseDAO.updateTickerFilesStatus(
          TickerFile.getSQLFileNames(
              tickerFiles.stream()
                  .filter(tickerFile -> tickerFile.getStatus() == status)
                  .collect(Collectors.toList())),
          status,
          Tables.TICKER_FILES.getTableName());
    } catch (ClickHouseException e) {
      throw new RuntimeException(e);
    }
  }
}
