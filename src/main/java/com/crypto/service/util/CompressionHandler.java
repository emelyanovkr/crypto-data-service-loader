package com.crypto.service.util;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.zip.GZIPOutputStream;

public class CompressionHandler {
  private static final int BUFFER_SIZE = 131072;

  public static void compressFilesWithGZIP(List<String> ticketsPath, PipedOutputStream pout, CountDownLatch latch) {
    long start = System.currentTimeMillis();
    System.out.println("Starting compress...");
    try (GZIPOutputStream gzOut = new GZIPOutputStream(pout)) {
      for (String file : ticketsPath) {
        try (InputStream fin = new FileInputStream(file)) {
          final byte[] buffer = new byte[BUFFER_SIZE];
          int n;
          while ((n = fin.read(buffer)) != -1) {
            gzOut.write(buffer, 0, n);
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    latch.countDown();
    System.out.println(
        "Compression finished, returning stream: " + (System.currentTimeMillis() - start));
  }
}
