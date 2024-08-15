package com.crypto.service.workers;

import com.clickhouse.client.ClickHouseException;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.dao.Tables;
import com.crypto.service.data.TickerFile;
import com.crypto.service.util.WorkersUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class ProcessingWorker implements Runnable {
  protected final Logger LOGGER = LoggerFactory.getLogger(ProcessingWorker.class);

  protected ClickHouseDAO clickHouseDAO;
  protected List<TickerFile> tickerFiles;

  public ProcessingWorker() {
    tickerFiles = new ArrayList<>();
    this.clickHouseDAO = new ClickHouseDAO();
  }

  @Override
  public void run() {
    retrieveTickerFilesInfo();
    proceedFilesStatus();
  }

  protected void retrieveTickerFilesInfo() {
    try {
      tickerFiles =
          clickHouseDAO.selectTickerFilesNamesOnStatus(
              Tables.TICKER_FILES.getTableName(),
              TickerFile.FileStatus.DISCOVERED,
              TickerFile.FileStatus.DOWNLOADING);
    } catch (ClickHouseException e) {
      LOGGER.error("ERROR RETRIEVING TICKER_FILES - ", e);
      throw new RuntimeException(e);
    }
  }

  protected void proceedFilesStatus() {
    LocalDate currentDate = LocalDate.now();

    int changesCounter = 0;
    for (TickerFile file : tickerFiles) {
      LocalDate fileDate = file.getCreateDate();
      if (fileDate.isEqual(currentDate) && file.getStatus() == TickerFile.FileStatus.DISCOVERED) {
        file.setStatus(TickerFile.FileStatus.DOWNLOADING);
        ++changesCounter;
      } else if (fileDate.isBefore(currentDate)) {
        file.setStatus(TickerFile.FileStatus.READY_FOR_PROCESSING);
        ++changesCounter;
      }
    }

    if (changesCounter > 0) {
      WorkersUtil.changeTickerFileUpdateStatus(clickHouseDAO, tickerFiles, TickerFile.FileStatus.DOWNLOADING);
      WorkersUtil.changeTickerFileUpdateStatus(clickHouseDAO, tickerFiles, TickerFile.FileStatus.READY_FOR_PROCESSING);
    }
  }
}
