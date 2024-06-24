package com.crypto.service.workers;

import com.clickhouse.client.ClickHouseException;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.dao.Tables;
import com.crypto.service.data.TickerFile;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class PreparingWorker implements Runnable {

  protected ClickHouseDAO clickHouseDAO;
  protected String directoryPath;
  protected List<TickerFile> fileNames;

  public PreparingWorker(String directoryPath) {
    this.clickHouseDAO = new ClickHouseDAO();
    this.directoryPath = directoryPath;
    fileNames = new ArrayList<>();
  }

  @Override
  public void run() {
    prepareFileNamesList();
  }

  protected void prepareFileNamesList() {
    try {
      LocalDate currentDate = LocalDate.now();
      LocalDate maxDate =
          clickHouseDAO.selectMaxTickerFilesDate("create_date", Tables.TICKER_FILES.getTableName());

      while (maxDate.isBefore(currentDate)) {
        Path dateDir = Paths.get(directoryPath + "/" + maxDate);
        if (Files.exists(dateDir)) {
          try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(dateDir)) {
            for (Path entry : directoryStream) {
              if (Files.isRegularFile(entry)) {
                fileNames.add(new TickerFile(entry.getFileName().toString(), maxDate, null));
              }
            }
          } catch (IOException e) {
            // TODO: logging?
            throw new RuntimeException(e);
          }
        }
        maxDate = maxDate.plusDays(1);
      }
    } catch (ClickHouseException e) {
      throw new RuntimeException(e);
    }

    startWatcherService();
  }

  protected void startWatcherService() {
    DirectoryWatcher directoryWatcher = new DirectoryWatcher(directoryPath, fileNames);
    Thread directoryThread = new Thread(directoryWatcher, "DIRECTORY-WATCHER-THREAD");
    directoryThread.start();
  }
}
