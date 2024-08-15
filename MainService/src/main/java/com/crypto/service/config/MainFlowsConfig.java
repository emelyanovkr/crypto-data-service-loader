package com.crypto.service.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MainFlowsConfig {

  @JsonProperty("DiscoverNewFilesConfig")
  protected DiscoverNewFilesConfig discoverNewFilesConfig;

  @JsonProperty("ProceedFilesStatusConfig")
  protected ProceedFilesStatusConfig proceedFilesStatusConfig;

  @JsonProperty("UploadTickersDataConfig")
  protected UploadTickersDataConfig uploadTickersDataConfig;

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

  public class DiscoverNewFilesConfig {
    protected int filesBufferSize;
    protected int flushDiscoveredFilesTimeoutSec;

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
  }

  public class ProceedFilesStatusConfig {
    protected int workCycleTimeSec;

    public int getWorkCycleTimeSec() {
      return workCycleTimeSec;
    }

    public void setWorkCycleTimeSec(int workCycleTimeSec) {
      this.workCycleTimeSec = workCycleTimeSec;
    }
  }

  public class UploadTickersDataConfig {
    protected int workCycleTimeSec;

    public int getWorkCycleTimeSec() {
      return workCycleTimeSec;
    }

    public void setWorkCycleTimeSec(int workCycleTimeSec) {
      this.workCycleTimeSec = workCycleTimeSec;
    }
  }
}
