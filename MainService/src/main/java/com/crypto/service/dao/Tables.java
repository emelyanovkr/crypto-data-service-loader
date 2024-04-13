package com.crypto.service.dao;

public enum Tables {
  TICKETS_DATA("tickets_data_db.tickets_data"),
  TICKETS_LOGS("tickets_data_db.tickets_logs");

  private final String tableName;

  Tables(String tableName) {
    this.tableName = tableName;
  }

  public String getTableName() {
    return tableName;
  }
}
