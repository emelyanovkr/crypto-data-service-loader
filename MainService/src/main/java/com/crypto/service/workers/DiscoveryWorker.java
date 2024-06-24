package com.crypto.service.workers;

import com.clickhouse.client.ClickHouseException;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.dao.Tables;
import com.crypto.service.data.TickerFile;

import java.util.*;

public class DiscoveryWorker implements Runnable {
  protected final List<TickerFile> localTickerFiles;

  protected final ClickHouseDAO clickHouseDAO;

  // TODO: test
  public DiscoveryWorker(List<TickerFile> filesBuffer) {
    this.localTickerFiles = new ArrayList<>(filesBuffer);

    this.clickHouseDAO = new ClickHouseDAO();
  }

  @Override
  public void run() {
    processDiscoveredFiles();
  }

  protected void processDiscoveredFiles() {
    try {
      List<String> filesFromDatabase =
          clickHouseDAO.selectTickerFilesNames(Tables.TICKER_FILES.getTableName());

      // TODO: DEBUG PRINT
      System.out.println("BEFORE: ");
      for (TickerFile localTickerFile : localTickerFiles) {
        System.out.println(
            localTickerFile.getFileName()
                + " "
                + localTickerFile.getCreateDate()
                + " "
                + localTickerFile.getStatus());
      }

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

      // TODO: DEBUG PRINT
      System.out.println("AFTER: ");
      for (TickerFile localTickerFile : localTickerFiles) {
        System.out.println(
            localTickerFile.getFileName()
                + " "
                + localTickerFile.getCreateDate()
                + " "
                + localTickerFile.getStatus());
      }

      clickHouseDAO.insertTickerFilesInfo(
          TickerFile.formDataToInsert(localTickerFiles), Tables.TICKER_FILES.getTableName());
    } catch (ClickHouseException e) {
      throw new RuntimeException(e);
    }
  }
}
