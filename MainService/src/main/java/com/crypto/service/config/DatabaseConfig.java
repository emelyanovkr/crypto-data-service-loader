package com.crypto.service.config;

public class DatabaseConfig {
  protected String host;
  protected int port;
  protected String username;
  protected String password;
  protected String database;
  protected String ssl;
  protected String customHttpParams;
  protected String socketTimeout;
  protected String socketKeepAlive;
  protected String connectTimeout;

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getSsl() {
    return ssl;
  }

  public void setSsl(String ssl) {
    this.ssl = ssl;
  }

  public String getCustomHttpParams() {
    return customHttpParams;
  }

  public void setCustomHttpParams(String customHttpParams) {
    this.customHttpParams = customHttpParams;
  }

  public String getSocketTimeout() {
    return socketTimeout;
  }

  public void setSocketTimeout(String socketTimeout) {
    this.socketTimeout = socketTimeout;
  }

  public String getSocketKeepAlive() {
    return socketKeepAlive;
  }

  public void setSocketKeepAlive(String socketKeepAlive) {
    this.socketKeepAlive = socketKeepAlive;
  }

  public String getConnectTimeout() {
    return connectTimeout;
  }

  public void setConnectTimeout(String connectTimeout) {
    this.connectTimeout = connectTimeout;
  }
}
