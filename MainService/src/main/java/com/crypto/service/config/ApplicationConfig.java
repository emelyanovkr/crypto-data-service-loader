package com.crypto.service.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ApplicationConfig {

  @JsonProperty("DatabaseConfig")
  protected DatabaseConfig databaseConfig;

  @JsonProperty("TickersDataConfig")
  protected TickersDataConfig tickersDataConfig;

  @JsonProperty("MainFlowsConfig")
  protected MainFlowsConfig mainFlowsConfig;

  public DatabaseConfig getDatabaseConfig() {
    return databaseConfig;
  }

  public void setDatabaseConfig(DatabaseConfig databaseConfig) {
    this.databaseConfig = databaseConfig;
  }

  public TickersDataConfig getTickersDataConfig() {
    return tickersDataConfig;
  }

  public void setTickersDataConfig(TickersDataConfig tickersDataConfig) {
    this.tickersDataConfig = tickersDataConfig;
  }

  public MainFlowsConfig getMainFlowsConfig() {
    return mainFlowsConfig;
  }

  public void setMainFlowsConfig(MainFlowsConfig mainFlowsConfig) {
    this.mainFlowsConfig = mainFlowsConfig;
  }
}
