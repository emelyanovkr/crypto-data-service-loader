package com.crypto.service.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MainFlowsConfig {

  @JsonProperty("DiscoverNewFilesConfig")
  protected DiscoverNewFilesConfig discoverNewFilesConfig;

  @JsonProperty("ProceedFilesStatusConfig")
  protected ProceedFilesStatusConfig proceedFilesStatusConfig;

  @JsonProperty("UploadTickersDataConfig")
  protected UploadTickersDataConfig uploadTickersDataConfig;

  @JsonProperty("CleanupUploadedFilesConfig")
  protected CleanupUploadedFilesConfig cleanupUploadedFilesConfig;

  public DiscoverNewFilesConfig getDiscoverNewFilesConfig() {
    return discoverNewFilesConfig;
  }

  public void setDiscoverNewFilesConfig(DiscoverNewFilesConfig discoverNewFilesConfig) {
    this.discoverNewFilesConfig = discoverNewFilesConfig;
  }

  public ProceedFilesStatusConfig getProceedFilesStatusConfig() {
    return proceedFilesStatusConfig;
  }

  public void setProceedFilesStatusConfig(ProceedFilesStatusConfig proceedFilesStatusConfig) {
    this.proceedFilesStatusConfig = proceedFilesStatusConfig;
  }

  public UploadTickersDataConfig getUploadTickersDataConfig() {
    return uploadTickersDataConfig;
  }

  public void setUploadTickersDataConfig(UploadTickersDataConfig uploadTickersDataConfig) {
    this.uploadTickersDataConfig = uploadTickersDataConfig;
  }

  public CleanupUploadedFilesConfig getCleanupUploadedFilesConfig() {
    return cleanupUploadedFilesConfig;
  }

  public void setCleanupUploadedFilesConfig(CleanupUploadedFilesConfig cleanupUploadedFilesConfig) {
    this.cleanupUploadedFilesConfig = cleanupUploadedFilesConfig;
  }

  public class DiscoverNewFilesConfig {
    protected int filesBufferSize;
    protected int flushDiscoveredFilesTimeoutSec;
    protected int sleepOnReconnectMs;
    protected int maxReconnectAttempts;

    public int getFilesBufferSize() {
      return filesBufferSize;
    }

    public void setFilesBufferSize(int filesBufferSize) {
      this.filesBufferSize = filesBufferSize;
    }

    public int getFlushDiscoveredFilesTimeoutSec() {
      return flushDiscoveredFilesTimeoutSec;
    }

    public void setFlushDiscoveredFilesTimeoutSec(int flushDiscoveredFilesTimeoutSec) {
      this.flushDiscoveredFilesTimeoutSec = flushDiscoveredFilesTimeoutSec;
    }

    public int getSleepOnReconnectMs() {
      return sleepOnReconnectMs;
    }

    public void setSleepOnReconnectMs(int sleepOnReconnectMs) {
      this.sleepOnReconnectMs = sleepOnReconnectMs;
    }

    public int getMaxReconnectAttempts() {
      return maxReconnectAttempts;
    }

    public void setMaxReconnectAttempts(int maxReconnectAttempts) {
      this.maxReconnectAttempts = maxReconnectAttempts;
    }
  }

  public class ProceedFilesStatusConfig {
    protected int workCycleTimeSec;
    protected int sleepOnReconnectMs;
    protected int maxReconnectAttempts;

    public int getWorkCycleTimeSec() {
      return workCycleTimeSec;
    }

    public void setWorkCycleTimeSec(int workCycleTimeSec) {
      this.workCycleTimeSec = workCycleTimeSec;
    }

    public int getSleepOnReconnectMs() {
      return sleepOnReconnectMs;
    }

    public void setSleepOnReconnectMs(int sleepOnReconnectMs) {
      this.sleepOnReconnectMs = sleepOnReconnectMs;
    }

    public int getMaxReconnectAttempts() {
      return maxReconnectAttempts;
    }

    public void setMaxReconnectAttempts(int maxReconnectAttempts) {
      this.maxReconnectAttempts = maxReconnectAttempts;
    }
  }

  public class UploadTickersDataConfig {
    protected int workCycleTimeSec;
    protected int sleepOnReconnectMs;
    protected int maxReconnectAttempts;

    public int getWorkCycleTimeSec() {
      return workCycleTimeSec;
    }

    public void setWorkCycleTimeSec(int workCycleTimeSec) {
      this.workCycleTimeSec = workCycleTimeSec;
    }

    public int getSleepOnReconnectMs() {
      return sleepOnReconnectMs;
    }

    public void setSleepOnReconnectMs(int sleepOnReconnectMs) {
      this.sleepOnReconnectMs = sleepOnReconnectMs;
    }

    public int getMaxReconnectAttempts() {
      return maxReconnectAttempts;
    }

    public void setMaxReconnectAttempts(int maxReconnectAttempts) {
      this.maxReconnectAttempts = maxReconnectAttempts;
    }
  }

  public class CleanupUploadedFilesConfig {
    protected int workCycleTimeHours;
    protected int sleepOnReconnectMs;
    protected int maxReconnectAttempts;

    public int getWorkCycleTimeHours() {
      return workCycleTimeHours;
    }

    public void setWorkCycleTimeHours(int workCycleTimeHours) {
      this.workCycleTimeHours = workCycleTimeHours;
    }

    public int getSleepOnReconnectMs() {
      return sleepOnReconnectMs;
    }

    public void setSleepOnReconnectMs(int sleepOnReconnectMs) {
      this.sleepOnReconnectMs = sleepOnReconnectMs;
    }

    public int getMaxReconnectAttempts() {
      return maxReconnectAttempts;
    }

    public void setMaxReconnectAttempts(int maxReconnectAttempts) {
      this.maxReconnectAttempts = maxReconnectAttempts;
    }
  }
}
