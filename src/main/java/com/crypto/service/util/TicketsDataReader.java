package com.crypto.service.util;

import com.crypto.service.dao.ClickHouseDAO;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class TicketsDataReader {

  private final int PARTS_QUANTITY = 32;
  private final int THREADS_COUNT = PARTS_QUANTITY;
  private final String SOURCE_PATH;

  private final Logger logger = LogManager.getLogger();

  public TicketsDataReader() {
    String currentDate = getCurrentDate();

    try
    {
      SOURCE_PATH = PropertiesLoader.loadProjectConfig().getProperty("DATA_PATH") + "/" + currentDate;
    } catch (IllegalArgumentException | IOException e)
    {
      logger.error("FAILED TO ACQUIRE PROPERTIES - {}", e.getMessage());
      throw new RuntimeException(e);
    }
  }

  private String getCurrentDate() {
    LocalDate currentDate = LocalDate.now();
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    return currentDate.format(formatter);
  }

  private List<String> getFilesInDirectory() {
    File searchDirectory = new File(SOURCE_PATH);
    List<String> directories;

    try
    {
      directories = List.of(Objects.requireNonNull(searchDirectory.list()));
    } catch (Exception e)
    {
      logger.error("FAILED SEARCH DIRECTORY - {}", e.getMessage());
      throw new RuntimeException();
    }

    return ImmutableList.copyOf(directories).stream()
        .map(fileName -> Paths.get(SOURCE_PATH, fileName).toString())
        .collect(Collectors.toList());
  }

  public void readExecutor() {
    List<String> ticketNames = getFilesInDirectory();

    List<List<String>> ticketParts =
        Lists.partition(ticketNames, ticketNames.size() / PARTS_QUANTITY);

    try (ExecutorService executor = Executors.newFixedThreadPool(THREADS_COUNT)) {

      ClickHouseDAO clickHouseDAO = ClickHouseDAO.getInstance();
      clickHouseDAO.truncateTable();
      clickHouseDAO.countRecords();

      for (List<String> ticketPartition : ticketParts) {
        PipedOutputStream pout = new PipedOutputStream();
        PipedInputStream pin = new PipedInputStream();
        pin.connect(pout);

        CompressionHandler handler = new CompressionHandler(pout);

        executor.execute(() -> handler.compressFilesWithGZIP(ticketPartition));
        executor.execute(() -> clickHouseDAO.insertFromCompressedFileStream(pin));
      }
    } catch (IOException e) {
      logger.error("FAILED TO CONNECT PIPED STREAMS - {}", e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
