package com.crypto.service.workers;

import java.io.IOException;
import java.nio.file.*;

import static java.nio.file.StandardWatchEventKinds.*;

public class DirectoryWatcher implements Runnable {
  String directoryPath;

  public DirectoryWatcher(String directoryPath) {
    this.directoryPath = directoryPath;
  }

  @Override
  public void run() {
    watchDirectory();
  }

  protected void watchDirectory() {
    try (WatchService watcher = FileSystems.getDefault().newWatchService()) {
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
            System.out.println("ENTRY CREATED: " + event.context().toString());
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
