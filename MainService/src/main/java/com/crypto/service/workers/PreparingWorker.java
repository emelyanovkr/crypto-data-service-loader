package com.crypto.service.workers;

import com.clickhouse.client.ClickHouseException;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.dao.Tables;
import com.crypto.service.data.TickerFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class PreparingWorker implements Runnable {

  protected final Logger LOGGER = LoggerFactory.getLogger(PreparingWorker.class);

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
    startWatcherService();
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
            LOGGER.error("ERROR OPENING DIRECTORY - ", e);
            throw new RuntimeException(e);
          }
        }
        maxDate = maxDate.plusDays(1);
      }
    } catch (ClickHouseException e) {
      LOGGER.error("ERROR MAX DATE ACQUIRING - ", e);
      throw new RuntimeException(e);
    }
  }

  protected void startWatcherService() {
    DirectoryWatcher directoryWatcher = new DirectoryWatcher(directoryPath, fileNames);
    Thread directoryThread = new Thread(directoryWatcher, "DIRECTORY-WATCHER-THREAD");
    directoryThread.start();
  }
}
