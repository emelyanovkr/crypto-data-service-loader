package com.crypto.service.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.*;
import java.text.DecimalFormat;
import java.util.List;
import java.util.zip.GZIPOutputStream;

public class CompressionHandler {
  private final int BUFFER_SIZE = 131072;
  private final PipedOutputStream pout;
  private final Logger LOGGER = LoggerFactory.getLogger(CompressionHandler.class);

  public CompressionHandler(PipedOutputStream pout) {
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
          LOGGER.error("READING COMPRESSION ERROR - ", e);
          throw new RuntimeException(e);
        }
      }
    } catch (IOException e) {
      LOGGER.error("COMPRESSION ERROR - ", e);
      throw new RuntimeException(e);
    }
    // TODO: Possible to implement TOTAL DATA INSERT, TOTAL RATE, TOTAL

    DecimalFormat df = new DecimalFormat("0.00");

    double totalTime = (double) (System.currentTimeMillis() - start) / 1000;
    totalSize /= (1024 * 1024);

    String totalTimeStr = df.format(totalTime);
    String totalSizeStr = df.format(totalSize);
    String compressionRate = df.format(totalSize / totalTime);

    MDC.put("data_size", totalSizeStr);
    MDC.put("compression_rate", compressionRate);
    MDC.put("total_time", totalTimeStr);

    LOGGER.info(
        "Compression of {} MB of data with rate {} MB/sec finished in {} sec.",
        totalSizeStr,
        compressionRate,
        totalTimeStr);
    MDC.clear();
  }
}