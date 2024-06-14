package com.crypto.service.data;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class TickerFile {
  protected String fileName;
  protected FileStatus status;

  public enum FileStatus {
    NOT_LOADED,
    DISCOVERED,
    IN_PROGRESS,
    FINISHED,
    ERROR;

    // for sql queries
    @Override
    public String toString()
    {
      return "'" + this.name() + "'";
    }
  }

  public TickerFile(String fileName, FileStatus status) {
    this.fileName = fileName;
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

  public static String formDataToInsert(Collection<TickerFile> data) {
    return data.stream()
        .map(tickerFile -> tickerFile.getFileName() + "\t" + tickerFile.getStatus())
        .collect(Collectors.joining("\n"));
  }

  public static String getFileNames(Collection<TickerFile> data) {
    return data.stream()
        .map(TickerFile::getFileName)
        .map(name -> "'" + name + "'")
        .collect(Collectors.joining(","));
  }
}
