package com.crypto.service.workers;

import com.clickhouse.client.ClickHouseException;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.dao.Tables;
import com.crypto.service.data.TickerFile;

import java.util.ArrayList;
import java.util.List;

// TODO: INTERFACE?
// TODO: rename to uploaderWorker
public class UploaderWorker implements Runnable {

  ClickHouseDAO clickHouseDAO;
  List<TickerFile> tickerFiles;

  public UploaderWorker() {
    this.clickHouseDAO = new ClickHouseDAO();
    tickerFiles = new ArrayList<>();
  }

  @Override
  public void run() {
    someFunction();
  }

  protected void someFunction() {
    try {
      tickerFiles =
          clickHouseDAO.selectTickerFilesNamesOnStatus(
              Tables.TICKER_FILES.getTableName(), TickerFile.FileStatus.READY_FOR_PROCESSING);

      clickHouseDAO.updateTickerFilesStatus(
          TickerFile.getSQLFileNames(tickerFiles),
          TickerFile.FileStatus.IN_PROGRESS,
          Tables.TICKER_FILES.getTableName());

      // TickersDataLoader

    } catch (ClickHouseException e) {
      throw new RuntimeException(e);
    }
  }
}
