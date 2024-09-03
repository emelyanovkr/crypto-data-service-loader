package com.crypto.service.workers;

import com.crypto.service.data.TickerFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.time.LocalDate;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.file.StandardWatchEventKinds.*;

public class DirectoryWatcher implements Runnable {
  protected final Logger LOGGER = LoggerFactory.getLogger(DirectoryWatcher.class);

  protected static final int FILES_BUFFER_SIZE = 8192;
  protected static final int DISCOVERY_FILES_TIMEOUT_SEC = 5;

  protected final String directoryPath;

  protected Queue<TickerFile> filesBuffer;
  protected final ReentrantLock filesBufferLock;

  public DirectoryWatcher(String directoryPath, List<TickerFile> initialFilesBuffer) {
    this.directoryPath = directoryPath;
    this.filesBuffer = new LinkedList<>(initialFilesBuffer);

    this.filesBufferLock = new ReentrantLock();

    Thread flushFilesBufferThread = new Thread(this::flushHandling, "FLUSH-FILES-BUFFER-THREAD");
    flushFilesBufferThread.setDaemon(true);
    flushFilesBufferThread.start();
  }

  protected void flushHandling() {
    long lastFlushTime = System.currentTimeMillis();
    while (true) {
      boolean fileBufferSizeSufficient;
      try {
        filesBufferLock.lock();
        fileBufferSizeSufficient = filesBuffer.size() > FILES_BUFFER_SIZE;
      } finally {
        filesBufferLock.unlock();
      }

      boolean fileTimeoutElapsed =
          System.currentTimeMillis() - lastFlushTime > DISCOVERY_FILES_TIMEOUT_SEC * 1000L;
      boolean flushRequired = fileTimeoutElapsed || fileBufferSizeSufficient;

      if (flushRequired) {
        Queue<TickerFile> filesBufferToFlush;
        try {
          filesBufferLock.lock();
          filesBufferToFlush = filesBuffer;
          filesBuffer = new LinkedList<>();
        } finally {
          filesBufferLock.unlock();
        }

        lastFlushTime = System.currentTimeMillis();
        startDiscoveryWorker(filesBufferToFlush);
      } else {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          LOGGER.error("Thread interrupted", e);
          throw new RuntimeException(e);
        }
      }
    }
  }

  protected void startDiscoveryWorker(Queue<TickerFile> filesBufferToFlush) {
    DiscoveryWorker discoveryWorker = new DiscoveryWorker(filesBufferToFlush);
    Thread discoveryThread = new Thread(discoveryWorker, "DISCOVERY-WORKER-THREAD");
    discoveryThread.start();
  }

  @Override
  public void run() {
    watchDirectory();
  }

  protected void watchDirectory() {
    try (WatchService watcher = FileSystems.getDefault().newWatchService()) {
      Path watchedDirectory = Path.of(directoryPath + "/" + LocalDate.now());

      watchedDirectory.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);

      LOGGER.info("Directory watcher started - {}", watchedDirectory);

      while (true) {
        WatchKey key;
        try {
          key = watcher.take();
        } catch (InterruptedException e) {
          LOGGER.error("Thread interrupted at KEY TAKE", e);
          throw new RuntimeException(e);
        }

        try {
          filesBufferLock.lock();

          for (WatchEvent<?> event : key.pollEvents()) {
            WatchEvent.Kind<?> kind = event.kind();

            if (kind == ENTRY_CREATE) {
              filesBuffer.add(new TickerFile(event.context().toString(), LocalDate.now(), null));
            }
            // else if (kind == ENTRY_DELETE) {
            //   System.out.println("ENTRY DELETED: " + event.context().toString());
            // } else if (kind == ENTRY_MODIFY) {
            //   System.out.println("ENTRY MODIFIED: " + event.context().toString());
            // }
          }
        } finally {
          filesBufferLock.unlock();
        }

        boolean valid = key.reset();
        if (!valid) {
          break;
        }
      }
    } catch (IOException e) {
      LOGGER.error("ERROR WATCHING DIRECTORY - ", e);
      throw new RuntimeException(e);
    }
  }
}
