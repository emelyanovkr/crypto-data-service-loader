package com.crypto.service;

import com.crypto.service.config.ApplicationConfig;
import com.crypto.service.config.DatabaseConfig;
import com.crypto.service.config.MainFlowsConfig;
import com.crypto.service.config.TickersDataConfig;
import com.crypto.service.flow.CleanupUploadedFilesFlow;
import com.crypto.service.flow.ProceedFilesStatusFlow;
import com.crypto.service.flow.SaveNewFilesToDbFlow;
import com.crypto.service.flow.UploadTickerFilesStatusAndDataFlow;
import com.crypto.service.workers.PreparingWorker;
import com.crypto.service.workers.ProcessingWorker;
import com.crypto.service.workers.UploaderWorker;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.flower.conf.FlowExec;
import com.flower.conf.FlowFuture;
import com.flower.engine.Flower;
import com.google.common.io.Resources;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.io.IOException;
import java.util.concurrent.*;

public class MainApplication {

  public static final String CONFIG_NAME = "application.yaml";
  public static ApplicationConfig applicationConfig;
  public static TickersDataConfig tickersDataConfig;
  public static DatabaseConfig databaseConfig;
  public static MainFlowsConfig mainFlowsConfig;
  public static String DATA_PATH;

  public static void main(String[] args) {

    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    try {
      applicationConfig =
          mapper.readValue(Resources.getResource(CONFIG_NAME), ApplicationConfig.class);
      tickersDataConfig = applicationConfig.getTickersDataConfig();
      databaseConfig = applicationConfig.getDatabaseConfig();
      mainFlowsConfig = applicationConfig.getMainFlowsConfig();

      DATA_PATH = tickersDataConfig.getTickersDataUploaderConfig().getTickersDataPath();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    initAndLaunchFlows();
  }

  public static void initAndLaunchFlows() {
    ScheduledExecutorService executorService = Executors.newScheduledThreadPool(4);
    ListeningScheduledExecutorService scheduler = MoreExecutors.listeningDecorator(executorService);
    Flower flower = new Flower(scheduler);
    flower.registerFlow(SaveNewFilesToDbFlow.class);
    flower.registerFlow(ProceedFilesStatusFlow.class);
    flower.registerFlow(UploadTickerFilesStatusAndDataFlow.class);
    flower.registerFlow(CleanupUploadedFilesFlow.class);
    flower.initialize();

    FlowExec<SaveNewFilesToDbFlow> saveFilesExec = flower.getFlowExec(SaveNewFilesToDbFlow.class);
    FlowFuture<SaveNewFilesToDbFlow> saveFilesFuture =
        saveFilesExec.runFlow(new SaveNewFilesToDbFlow(DATA_PATH));

    FlowExec<ProceedFilesStatusFlow> proceedFilesExec =
        flower.getFlowExec(ProceedFilesStatusFlow.class);
    FlowFuture<ProceedFilesStatusFlow> proceedFilesFuture =
        proceedFilesExec.runFlow(new ProceedFilesStatusFlow());

    FlowExec<UploadTickerFilesStatusAndDataFlow> uploadFilesExec =
        flower.getFlowExec(UploadTickerFilesStatusAndDataFlow.class);
    FlowFuture<UploadTickerFilesStatusAndDataFlow> uploadFilesFuture =
        uploadFilesExec.runFlow(new UploadTickerFilesStatusAndDataFlow(DATA_PATH));

    FlowExec<CleanupUploadedFilesFlow> cleanUpFilesExec =
        flower.getFlowExec(CleanupUploadedFilesFlow.class);
    FlowFuture<CleanupUploadedFilesFlow> cleanUpFilesFuture =
        cleanUpFilesExec.runFlow(new CleanupUploadedFilesFlow(DATA_PATH));

    try {
      SaveNewFilesToDbFlow saveState = saveFilesFuture.getFuture().get();
      ProceedFilesStatusFlow proceedState = proceedFilesFuture.getFuture().get();
      UploadTickerFilesStatusAndDataFlow uploadState = uploadFilesFuture.getFuture().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}
