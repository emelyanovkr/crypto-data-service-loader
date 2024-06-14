package com.crypto.service.workers;

import com.clickhouse.client.ClickHouseException;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.dao.Tables;
import com.crypto.service.data.TickerFile;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

public class DiscoveryWorker implements Runnable {

  ClickHouseDAO clickHouseDAO;

  // TODO: Pass by atomic Reference
  List<TickerFile> localTickerFiles = new ArrayList<>();
  String directory;

  public DiscoveryWorker(String directory) {
    this.directory = directory;

    this.clickHouseDAO = new ClickHouseDAO();
  }

  // TODO: temporary
  protected List<TickerFile> getFilesInDirectory(String directory) {
    File searchDirectory = new File(directory);
    List<String> tickerNames;

    try {
      tickerNames = List.of(Objects.requireNonNull(searchDirectory.list()));
    } catch (Exception e) {
      // TODO: logging?
      throw new RuntimeException();
    }

    return tickerNames.stream()
        .map(fileName -> new TickerFile(fileName, TickerFile.FileStatus.NOT_LOADED))
        .collect(Collectors.toList());
  }

  @Override
  public void run() {
    someMethod();
  }

  protected void someMethod() {
    this.localTickerFiles = getFilesInDirectory(directory);

    try {
      List<String> filesFromDatabase =
          clickHouseDAO.selectTickerFilesNames(Tables.TICKER_FILES.getTableName());
      setDiffStatus(filesFromDatabase);
    } catch (ClickHouseException e) {
      throw new RuntimeException(e);
    }
  }

  protected void setDiffStatus(List<String> filesFromDatabase) {
    Set<String> filesInDatabase = new HashSet<>(filesFromDatabase);
    for (TickerFile localTickerFile : localTickerFiles) {
      if (filesInDatabase.contains(localTickerFile.getFileName())) {
        localTickerFile.setStatus(TickerFile.FileStatus.DISCOVERED);
      }
    }
    try {
      clickHouseDAO.updateTickerFilesStatus(
          TickerFile.getFileNames(localTickerFiles),
          TickerFile.FileStatus.DISCOVERED,
          Tables.TICKER_FILES.getTableName());
    } catch (ClickHouseException e) {
      throw new RuntimeException(e);
    }
  }
}
