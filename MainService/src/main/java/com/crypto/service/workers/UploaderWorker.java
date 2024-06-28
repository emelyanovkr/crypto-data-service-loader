package com.crypto.service.workers;

import com.clickhouse.client.ClickHouseException;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.dao.Tables;
import com.crypto.service.data.TickerFile;
import com.crypto.service.data.TickersDataLoader;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class UploaderWorker implements Runnable {

  protected String directoryPath;
  protected ClickHouseDAO clickHouseDAO;
  protected List<TickerFile> tickerFiles;
  protected List<Path> filePaths;

  public UploaderWorker(String directoryPath) {
    this.directoryPath = directoryPath;
    this.clickHouseDAO = new ClickHouseDAO();
    tickerFiles = new ArrayList<>();
  }

  @Override
  public void run() {
    retrievePreparedFiles();
    fillPathsList();
    uploadTickerFilesData();
  }

  protected void retrievePreparedFiles() {
    try {
      tickerFiles =
          clickHouseDAO.selectTickerFilesNamesOnStatus(
              Tables.TICKER_FILES.getTableName(), TickerFile.FileStatus.READY_FOR_PROCESSING);

      clickHouseDAO.updateTickerFilesStatus(
          TickerFile.getSQLFileNames(tickerFiles),
          TickerFile.FileStatus.IN_PROGRESS,
          Tables.TICKER_FILES.getTableName());

    } catch (ClickHouseException e) {
      throw new RuntimeException(e);
    }
  }

  protected void fillPathsList() {
    filePaths = new ArrayList<>();
    HashMap<String, HashSet<String>> tickerDatesToFileNames =
        tickerFiles.stream()
            .collect(
                Collectors.groupingBy(
                    tickerFile -> tickerFile.getCreateDate().toString(),
                    HashMap::new,
                    Collectors.mapping(
                        TickerFile::getFileName, Collectors.toCollection(HashSet::new))));

    try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Path.of(directoryPath))) {
      for (Path pathEntry : directoryStream) {
        String createDateDirectory = pathEntry.getFileName().toString();

        if (Files.isDirectory(pathEntry) && tickerDatesToFileNames.containsKey(createDateDirectory)) {
          try (DirectoryStream<Path> innerDirectoryStream = Files.newDirectoryStream(pathEntry)) {
            for (Path innerPathEntry : innerDirectoryStream) {
              if (tickerDatesToFileNames
                  .get(createDateDirectory)
                  .contains(innerPathEntry.getFileName().toString())) {
                filePaths.add(innerPathEntry.toAbsolutePath());
              }
            }
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // TODO: DEBUG PRINT
    System.out.println(filePaths.size() + " | " + tickerFiles.size());
  }

  protected void uploadTickerFilesData()
  {
    TickersDataLoader dataLoader = new TickersDataLoader(filePaths, tickerFiles);
    dataLoader.uploadTickersData();
  }
}
