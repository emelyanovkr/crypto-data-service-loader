package com.crypto.service.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.zip.GZIPOutputStream;

public class CompressionHandler {
  private final int BUFFER_SIZE = 131072;
  private final PipedOutputStream pout;
  private final Logger LOGGER = LoggerFactory.getLogger(CompressionHandler.class);

  public CompressionHandler(PipedOutputStream pout)
  {
    this.pout = pout;
  }

  public void compressFilesWithGZIP(List<String> ticketsPath) {
    long start = System.currentTimeMillis();
    double totalSize = 0;

    try (GZIPOutputStream gzOut = new GZIPOutputStream(pout)) {
      for (String file : ticketsPath) {
        try (InputStream fin = new FileInputStream(file)) {
          final byte[] buffer = new byte[BUFFER_SIZE];
          int n;
          while ((n = fin.read(buffer)) != -1) {
            gzOut.write(buffer, 0, n);
            totalSize += n;
          }
        } catch (IOException e) {
          LOGGER.error(e.getMessage());
          throw new RuntimeException(e);
        }
      }
    } catch (IOException e) {
      LOGGER.error(e.getMessage());
      throw new RuntimeException(e);
    }
    // TODO: Possible to implement TOTAL DATA INSERT, TOTAL RATE, TOTAL TIME

    double totalTime = (double) (System.currentTimeMillis() - start) / 1000;
    totalSize /= (1024 * 1024);
    double compressionRate = totalSize / totalTime;
    LOGGER.info(String.format("Compression of %.2f MB of data with rate %.2f MB/sec finished in %.2f sec.",
      totalSize, compressionRate, totalTime));
  }
}
