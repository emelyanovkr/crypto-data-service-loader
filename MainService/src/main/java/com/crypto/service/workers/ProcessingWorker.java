package com.crypto.service.workers;

import com.clickhouse.client.ClickHouseException;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.dao.Tables;
import com.crypto.service.data.TickerFile;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ProcessingWorker implements Runnable {

  protected ClickHouseDAO clickHouseDAO;
  protected List<TickerFile> tickerFiles;

  public ProcessingWorker() {
    tickerFiles = new ArrayList<>();
    this.clickHouseDAO = new ClickHouseDAO();
  }

  @Override
  public void run() {
    retrieveTickerFilesInfo();
  }

  protected void retrieveTickerFilesInfo() {
    try {
      tickerFiles =
          clickHouseDAO.selectTickerFilesNamesOnStatus(
              Tables.TICKER_FILES.getTableName(),
              TickerFile.FileStatus.DISCOVERED,
              TickerFile.FileStatus.DOWNLOADING);
    } catch (ClickHouseException e) {
      throw new RuntimeException(e);
    }
    checkFilesStatus();
  }

  protected void checkFilesStatus() {
    LocalDate currentDate = LocalDate.now();

    int changesCounter = 0;
    for (TickerFile file : tickerFiles) {
      LocalDate fileDate = TickerFile.getFileDate(file.getFileName());
      if (fileDate.isEqual(currentDate) && file.getStatus() != TickerFile.FileStatus.DOWNLOADING) {
        file.setStatus(TickerFile.FileStatus.DOWNLOADING);
        ++changesCounter;
      } else if (fileDate.isBefore(currentDate)
          && file.getStatus() != TickerFile.FileStatus.READY_FOR_PROCESSING) {
        file.setStatus(TickerFile.FileStatus.READY_FOR_PROCESSING);
        ++changesCounter;
      }
    }

    if(changesCounter > 0) {
      proceedToUpdateStatus(TickerFile.FileStatus.DOWNLOADING);
      proceedToUpdateStatus(TickerFile.FileStatus.READY_FOR_PROCESSING);
    }
  }

  protected void proceedToUpdateStatus(TickerFile.FileStatus status) {
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
