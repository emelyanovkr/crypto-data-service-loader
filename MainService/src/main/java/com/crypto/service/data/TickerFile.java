package com.crypto.service.data;

import java.time.LocalDate;
import java.util.Collection;
import java.util.Objects;
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

    public static FileStatus parseStatus(String status) {
      return switch (status) {
        case "DISCOVERED" -> FileStatus.DISCOVERED;
        case "DOWNLOADING" -> FileStatus.DOWNLOADING;
        case "READY_FOR_PROCESSING" -> FileStatus.READY_FOR_PROCESSING;
        case "IN_PROGRESS" -> FileStatus.IN_PROGRESS;
        case "FINISHED" -> FileStatus.FINISHED;
        case "ERROR" -> FileStatus.ERROR;
        default -> throw new IllegalArgumentException("Unknown status: " + status);
      };
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TickerFile that = (TickerFile) o;
    return Objects.equals(fileName, that.fileName)
        && Objects.equals(createDate, that.createDate)
        && status == that.status;
  }

  @Override
  public int hashCode() {
    return Objects.hash(fileName, createDate, status);
  }
}
