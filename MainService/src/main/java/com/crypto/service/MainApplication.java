package com.crypto.service;

import com.crypto.service.workers.PreparingWorker;
import com.crypto.service.workers.ProcessingWorker;
import com.crypto.service.workers.UploaderWorker;

public class MainApplication {
  public static void main(String[] args) {

    // PreparingWorker preparingWorker = new PreparingWorker("F:\\Programming\\Java\\DATA\\TestingData");
    // Thread preparingThread = new Thread(preparingWorker, "PREPARING-WORKER-THREAD");
    // preparingThread.start();

    // TODO: DEFINE TIMINGS FOR LAUNCH THREADS
    // ProcessingWorker processingWorker = new ProcessingWorker();
    // Thread processingThread = new Thread(processingWorker);
    // processingThread.start();

    UploaderWorker preparingWorker = new UploaderWorker("F:\\Programming\\Java\\DATA\\TestingData");
    Thread preparingThread = new Thread(preparingWorker);
    preparingThread.start();

  }
}
