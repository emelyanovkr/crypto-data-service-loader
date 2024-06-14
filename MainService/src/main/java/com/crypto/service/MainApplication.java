package com.crypto.service;

import com.crypto.service.data.TickersDataLoader;
import com.crypto.service.workers.DirectoryWatcher;
import com.crypto.service.workers.DiscoveryWorker;
import com.crypto.service.workers.ProcessingWorker;

public class MainApplication {
  public static void main(String[] args) {
    // TickersDataLoader dataLoader = new TickersDataLoader();
    // dataLoader.uploadTickersData();

    // DiscoveryWorker discoveryWorker = new DiscoveryWorker("F:\\Programming\\Java\\DATA\\TestData");
    // Thread discoveryThread = new Thread(discoveryWorker);
    // discoveryThread.start();

    // DirectoryWatcher directoryWatcher = new DirectoryWatcher("F:\\Programming\\Java\\DATA\\TestData");
    // Thread watcherThread = new Thread(directoryWatcher);
    // watcherThread.start();

    ProcessingWorker processingWorker = new ProcessingWorker();
    Thread processingThread = new Thread(processingWorker);
    processingThread.start();



  }
}
