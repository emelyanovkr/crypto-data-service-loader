package com.crypto.service.workers;

import com.crypto.service.data.TickerFile;

import java.io.IOException;
import java.nio.file.*;
import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.file.StandardWatchEventKinds.*;

public class DirectoryWatcher implements Runnable {
  protected final String directoryPath;
  protected final AtomicReference<ConcurrentLinkedQueue<TickerFile>> filesBuffer;

  protected final int FILES_BUFFER_SIZE = 8192;
  // TODO: CHANGE THIS TIMEOUT
  protected final int DISCOVERY_FILES_TIMEOUT_SEC = 5;

  public DirectoryWatcher(String directoryPath, List<TickerFile> filesBuffer) {
    this.directoryPath = directoryPath;
    this.filesBuffer = new AtomicReference<>();
    this.filesBuffer.set(new ConcurrentLinkedQueue<>(filesBuffer));

    Thread flushFilesBufferThread = new Thread(this::flushHandling, "FLUSH-FILES-BUFFER-THREAD");
    flushFilesBufferThread.setDaemon(true);
    flushFilesBufferThread.start();
  }

  protected boolean filesFlushRequired(long lastFlushTime) {
    boolean fileTimeoutElapsed =
        System.currentTimeMillis() - lastFlushTime > DISCOVERY_FILES_TIMEOUT_SEC * 1000L;
    boolean fileBufferSizeSufficient = filesBuffer.get().size() > FILES_BUFFER_SIZE;

    return fileTimeoutElapsed || fileBufferSizeSufficient;
  }

  protected void flushHandling() {
    long lastFlushTime = System.currentTimeMillis();
    while (true) {
      if (filesFlushRequired(lastFlushTime)) {
        // TODO: DEBUG PRINT
        System.out.println("GOING TO FLUSH");
        lastFlushTime = System.currentTimeMillis();
        startDiscoveryWorker();
        filesBuffer.get().clear();
      } else {
        try {
          // TODO: TO look over time
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @Override
  public void run() {
    watchDirectory();
  }

  protected void startDiscoveryWorker() {
    DiscoveryWorker discoveryWorker = new DiscoveryWorker(filesBuffer.get().stream().toList());
    Thread discoveryThread = new Thread(discoveryWorker, "DISCOVERY-WORKER-THREAD");
    discoveryThread.start();
  }

  protected void watchDirectory() {
    try (WatchService watcher = FileSystems.getDefault().newWatchService()) {
      // TODO: нужно чтобы следил за директорией с датой определённой а не за всей директорией
      Path watchedDirectory = Paths.get(directoryPath);

      watchedDirectory.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);

      System.out.println("Directory watcher started: " + watchedDirectory);

      while (true) {
        WatchKey key;
        try {
          key = watcher.take();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }

        for (WatchEvent<?> event : key.pollEvents()) {
          WatchEvent.Kind<?> kind = event.kind();

          if (kind == ENTRY_CREATE) {
            filesBuffer
                .get()
                .add(new TickerFile(event.context().toString(), LocalDate.now(), null));
          } else if (kind == ENTRY_DELETE) {
            System.out.println("ENTRY DELETED: " + event.context().toString());
          } else if (kind == ENTRY_MODIFY) {
            System.out.println("ENTRY MODIFIED: " + event.context().toString());
          }
        }

        boolean valid = key.reset();
        if (!valid) {
          break;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
