package com.crypto.service.util;

import com.google.common.collect.ImmutableList;

import java.io.File;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;

public class TicketsDataReader {
  private static String getCurrentDate() {
    LocalDate currentDate = LocalDate.now();
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    return currentDate.format(formatter);
  }

  private static List<String> getFilesInDirectory() {
    String currentDate = getCurrentDate();
    String sourcePath =
        PropertiesLoader.loadJDBCProp().getProperty("DATA_PATH") + "/" + currentDate;

    File searchDirectory = new File(sourcePath);
    return ImmutableList.copyOf(Objects.requireNonNull(searchDirectory.list()));
  }
}
