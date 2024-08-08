package com.crypto.service;

import com.crypto.service.flow.ProceedFilesStatusFlow;
import com.crypto.service.flow.SaveNewFilesToDbFlow;
import com.crypto.service.flow.UploadTickerFilesStatusAndDataFlow;
import com.crypto.service.workers.PreparingWorker;
import com.crypto.service.workers.ProcessingWorker;
import com.crypto.service.workers.UploaderWorker;
import com.flower.conf.FlowExec;
import com.flower.conf.FlowFuture;
import com.flower.engine.Flower;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.util.concurrent.*;

public class MainApplication {
  public static void main(String[] args) {

    // TODO: DEFINE ALL TIMINGS IN PROPERTIES FILES

    ScheduledExecutorService executorService = Executors.newScheduledThreadPool(4);
    ListeningScheduledExecutorService scheduler = MoreExecutors.listeningDecorator(executorService);
    Flower flower = new Flower(scheduler);
    flower.registerFlow(SaveNewFilesToDbFlow.class);
    flower.registerFlow(ProceedFilesStatusFlow.class);
    flower.registerFlow(UploadTickerFilesStatusAndDataFlow.class);
    flower.initialize();

    /*FlowExec<SaveNewFilesToDbFlow> saveFilesExec = flower.getFlowExec(SaveNewFilesToDbFlow.class);
     FlowFuture<SaveNewFilesToDbFlow> saveFilesFuture =
       saveFilesExec.runFlow(new SaveNewFilesToDbFlow("F:\\Programming\\Java\\DATA\\TestData"));

    FlowExec<ProceedFilesStatusFlow> proceedFilesExec =
        flower.getFlowExec(ProceedFilesStatusFlow.class);
    FlowFuture<ProceedFilesStatusFlow> proceedFilesFuture =
        proceedFilesExec.runFlow(new ProceedFilesStatusFlow());*/

    FlowExec<UploadTickerFilesStatusAndDataFlow> uploadFilesExec =
        flower.getFlowExec(UploadTickerFilesStatusAndDataFlow.class);
    FlowFuture<UploadTickerFilesStatusAndDataFlow> uploadFilesFuture =
        uploadFilesExec.runFlow(
            new UploadTickerFilesStatusAndDataFlow("F:\\Programming\\Java\\DATA\\TestData"));

    try {
      // SaveNewFilesToDbFlow saveState = saveFilesFuture.getFuture()
      // ProceedFilesStatusFlow proceedState = proceedFilesFuture.getFuture().get();
      UploadTickerFilesStatusAndDataFlow uploadState = uploadFilesFuture.getFuture().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    // PreparingWorker preparingWorker = new
    // PreparingWorker("F:\\Programming\\Java\\DATA\\TestData");
    // Thread preparingThread = new Thread(preparingWorker, "PREPARING-WORKER-THREAD");
    // preparingThread.start();

    // TODO: DEFINE TIMINGS FOR LAUNCH THREADS
    // ProcessingWorker processingWorker = new ProcessingWorker();
    // Thread processingThread = new Thread(processingWorker);
    // processingThread.start();

    // UploaderWorker preparingWorker = new
    // UploaderWorker("F:\\Programming\\Java\\DATA\\TestingData");
    // Thread preparingThread = new Thread(preparingWorker);
    // preparingThread.start();

  }
}
