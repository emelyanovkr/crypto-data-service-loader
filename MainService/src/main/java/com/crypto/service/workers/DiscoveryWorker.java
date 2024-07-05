package com.crypto.service.workers;

import com.clickhouse.client.ClickHouseException;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.dao.Tables;
import com.crypto.service.data.TickerFile;

import java.util.*;

public class DiscoveryWorker implements Runnable {
  protected final Collection<TickerFile> localTickerFiles;

  protected final ClickHouseDAO clickHouseDAO;

  public DiscoveryWorker(Collection<TickerFile> filesBuffer) {
    this.localTickerFiles = filesBuffer;
    this.clickHouseDAO = new ClickHouseDAO();
  }

  @Override
  public void run() {
    processDiscoveredFiles();
  }

  protected void processDiscoveredFiles() {
    try {
      List<String> filesFromDatabase =
          clickHouseDAO.selectExclusiveTickerFilesNames(
              TickerFile.getSQLFileNames(localTickerFiles), Tables.TICKER_FILES.getTableName());

      System.out.println(filesFromDatabase);

      Set<String> filesInDatabase = new HashSet<>(filesFromDatabase);
      for (Iterator<TickerFile> localIterator = localTickerFiles.iterator();
          localIterator.hasNext(); ) {
        TickerFile localTickerFile = localIterator.next();
        if (!filesInDatabase.contains(localTickerFile.getFileName())) {
          localTickerFile.setStatus(TickerFile.FileStatus.DISCOVERED);
        } else {
          localIterator.remove();
        }
      }

      clickHouseDAO.insertTickerFilesInfo(
          TickerFile.formDataToInsert(localTickerFiles), Tables.TICKER_FILES.getTableName());
    } catch (ClickHouseException e) {
      throw new RuntimeException(e);
    }
  }
}
