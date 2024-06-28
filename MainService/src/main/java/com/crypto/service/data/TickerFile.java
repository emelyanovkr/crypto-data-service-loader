package com.crypto.service.data;

import java.time.LocalDate;
import java.util.Collection;
import java.util.stream.Collectors;

public class TickerFile {
  protected final String fileName;
  protected final LocalDate createDate;
  protected FileStatus status;

  public enum FileStatus {
    DISCOVERED,
    DOWNLOADING,
    READY_FOR_PROCESSING,
    IN_PROGRESS,
    FINISHED,
    ERROR;

    // for sql queries
    public String getSQLStatus() {
      return "'" + this.name() + "'";
    }
  }

  public TickerFile(String fileName, LocalDate createDate, FileStatus status) {
    this.fileName = fileName;
    this.createDate = createDate;
    this.status = status;
  }

  public void setStatus(FileStatus status) {
    this.status = status;
  }

  public String getFileName() {
    return fileName;
  }

  public FileStatus getStatus() {
    return status;
  }

  public LocalDate getCreateDate() {
    return createDate;
  }

  public static String formDataToInsert(Collection<TickerFile> data) {
    return data.stream()
        .map(
            tickerFile ->
                tickerFile.getFileName()
                    + "\t"
                    + tickerFile.getCreateDate()
                    + "\t"
                    + tickerFile.getStatus())
        .collect(Collectors.joining("\n"));
  }

  public static String getSQLFileNames(Collection<TickerFile> data) {
    return data.stream()
        .map(TickerFile::getFileName)
        .map(name -> "'" + name + "'")
        .collect(Collectors.joining(","));
  }
}
