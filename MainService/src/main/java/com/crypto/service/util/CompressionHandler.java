package com.crypto.service.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.*;
import java.text.DecimalFormat;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPOutputStream;

public class CompressionHandler {
  protected final int BUFFER_SIZE = 131072;
  protected final PipedOutputStream pout;
  protected final Logger LOGGER = LoggerFactory.getLogger(CompressionHandler.class);

  protected final AtomicBoolean taskRunningStatus;
  protected final AtomicBoolean stopCommand;

  protected CompressionHandler(
      PipedOutputStream pout, AtomicBoolean taskRunningStatus, AtomicBoolean stopCommand) {
    this.pout = pout;
    this.taskRunningStatus = taskRunningStatus;
    this.stopCommand = stopCommand;
  }

  public static CompressionHandler createCompressionHandler(
      PipedOutputStream pout, AtomicBoolean taskRunningStatus, AtomicBoolean stopCommand) {
    return new CompressionHandler(pout, taskRunningStatus, stopCommand);
  }

  public void compressFilesWithGZIP(List<String> tickersPath) {
    if (tickersPath == null || tickersPath.isEmpty()) {
      return;
    }

    try {
      long start = System.currentTimeMillis();
      double totalDataCompressedSize = 0;

      try (GZIPOutputStream gzOut = new GZIPOutputStream(pout)) {
        for (String file : tickersPath) {
          try (InputStream fin = new FileInputStream(file)) {
            final byte[] buffer = new byte[BUFFER_SIZE];
            int n;
            while ((n = fin.read(buffer)) != -1) {
              if (stopCommand.get()) {
                return;
              }
              gzOut.write(buffer, 0, n);
              totalDataCompressedSize += n;
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
      // TODO: Possible to implement TOTAL DATA INSERT, TOTAL RATE, TOTAL TIME & INSERT SESSION
      //  UUID?

      DecimalFormat df = new DecimalFormat("0.00");

      double totalTime = (double) (System.currentTimeMillis() - start) / 1000;
      totalDataCompressedSize /= (1024 * 1024);

      String totalTimeStr = df.format(totalTime);
      String totalSizeStr = df.format(totalDataCompressedSize);
      String compressionRate = df.format(totalDataCompressedSize / totalTime);

      MDC.put("data_size", totalSizeStr);
      MDC.put("compression_rate", compressionRate);
      MDC.put("total_time", totalTimeStr);

      LOGGER.info(
          "Compression of {} MB of data with rate {} MB/sec finished in {} sec.",
          totalSizeStr,
          compressionRate,
          totalTimeStr);

      MDC.clear();
    } finally {
      taskRunningStatus.set(false);
    }
  }
}