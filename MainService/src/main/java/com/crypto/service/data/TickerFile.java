package com.crypto.service.data;

public class TickerFile
{
  protected String fileName;
  protected FileStatus status;

  public enum FileStatus {
    NOT_LOADED,
    IN_PROGRESS,
    FINISHED,
    ERROR;
  }

  public TickerFile(String fileName, FileStatus status) {
    this.fileName = fileName;
    this.status = status;
  }

  public String getFileName() {
    return fileName;
  }

  public FileStatus getStatus() {
    return status;
  }
}
