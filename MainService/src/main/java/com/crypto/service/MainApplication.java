package com.crypto.service;

import com.crypto.service.workers.PreparingWorker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

public class MainApplication {
  public static void main(String[] args) {
    // TickersDataLoader dataLoader = new TickersDataLoader();
    // dataLoader.uploadTickersData();

    PreparingWorker preparingWorker = new PreparingWorker("F:\\Programming\\Java\\DATA\\TestData");
    Thread preparingThread = new Thread(preparingWorker, "PREPARING-WORKER-THREAD");
    preparingThread.start();

    // ProcessingWorker processingWorker = new ProcessingWorker();
    // Thread processingThread = new Thread(processingWorker);
    // processingThread.start();

    // UploaderWorker preparingWorker = new UploaderWorker();
    // Thread preparingThread = new Thread(preparingWorker);
    // preparingThread.start();

  }
}
