package com.crypto.service.dao;

public enum Tables {
  TICKERS_DATA("tickers_data_db.tickers_data"),
  TICKERS_LOGS("tickers_data_db.tickers_logs"),
  TICKER_FILES("tickers_data_db.ticker_files");

  private final String tableName;

  Tables(String tableName) {
    this.tableName = tableName;
  }

  public String getTableName() {
    return tableName;
  }
}
