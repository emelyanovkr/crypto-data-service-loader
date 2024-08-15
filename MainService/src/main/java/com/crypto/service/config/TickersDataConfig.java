package com.crypto.service.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TickersDataConfig {

  @JsonProperty("TickersDataUploaderConfig")
  protected TickersDataUploaderConfig tickersDataUploaderConfig;

  @JsonProperty("CompressionHandlerConfig")
  protected CompressionHandlerConfig compressionHandlerConfig;

  public TickersDataUploaderConfig getTickersDataUploaderConfig() {
    return tickersDataUploaderConfig;
  }

  public void setTickersDataUploaderConfig(TickersDataUploaderConfig tickersDataUploaderConfig) {
    this.tickersDataUploaderConfig = tickersDataUploaderConfig;
  }

  public CompressionHandlerConfig getCompressionHandlerConfig() {
    return compressionHandlerConfig;
  }

  public void setCompressionHandlerConfig(CompressionHandlerConfig compressionHandlerConfig) {
    this.compressionHandlerConfig = compressionHandlerConfig;
  }

  public class TickersDataUploaderConfig {
    protected int maxFlushDataAttempts;
    protected int divideDataPartsQuantity;
    protected String tickersDataPath;
    protected int sleepOnReconnectMs;

    public int getMaxFlushDataAttempts() {
      return maxFlushDataAttempts;
    }

    public void setMaxFlushDataAttempts(int maxFlushDataAttempts) {
      this.maxFlushDataAttempts = maxFlushDataAttempts;
    }

    public int getDivideDataPartsQuantity() {
      return divideDataPartsQuantity;
    }

    public void setDivideDataPartsQuantity(int divideDataPartsQuantity) {
      this.divideDataPartsQuantity = divideDataPartsQuantity;
    }

    public String getTickersDataPath() {
      return tickersDataPath;
    }

    public void setTickersDataPath(String tickersDataPath) {
      this.tickersDataPath = tickersDataPath;
    }

    public int getSleepOnReconnectMs() {
      return sleepOnReconnectMs;
    }

    public void setSleepOnReconnectMs(int sleepOnReconnectMs) {
      this.sleepOnReconnectMs = sleepOnReconnectMs;
    }
  }

  public class CompressionHandlerConfig {
    protected int compressionBufferSize;

    public int getCompressionBufferSize() {
      return compressionBufferSize;
    }

    public void setCompressionBufferSize(int compressionBufferSize) {
      this.compressionBufferSize = compressionBufferSize;
    }
  }
}
