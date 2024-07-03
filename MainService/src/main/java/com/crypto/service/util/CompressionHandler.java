package com.crypto.service.util;

import com.crypto.service.data.TickerFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.*;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.util.HashSet;
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

  public void compressFilesWithGZIP(List<Path> tickersPath, HashSet<TickerFile> tickerFiles) {
    if (tickersPath == null || tickersPath.isEmpty()) {
      return;
    }

    try {
      long start = System.currentTimeMillis();
      double totalDataCompressedSize = 0;

      try (GZIPOutputStream gzOut = new GZIPOutputStream(pout)) {
        for (Path filePath : tickersPath) {
          String fileName = filePath.getFileName().toString();

          try (InputStream fin = new FileInputStream(filePath.toFile())) {
            final byte[] buffer = new byte[BUFFER_SIZE];
            int n;
            while ((n = fin.read(buffer)) != -1) {
              if (stopCommand.get()) {
                return;
              }
              gzOut.write(buffer, 0, n);
              totalDataCompressedSize += n;
            }

            // TODO: NULL CHECK?
            TickerFile representingFile =
                tickerFiles.stream()
                    .filter(tickerFile -> tickerFile.getFileName().equals(fileName))
                    .findFirst()
                    .get();
            representingFile.setStatus(TickerFile.FileStatus.FINISHED);

          } catch (IOException e) {

            LOGGER.error("READING COMPRESSION ERROR - ", e);
            throw new RuntimeException(e);
          }
        }
      } catch (IOException e) {
        LOGGER.error("COMPRESSION ERROR - ", e);
        throw new RuntimeException(e);
      }

      formLoggingData(start, totalDataCompressedSize);
    } finally {
      taskRunningStatus.set(false);
    }
  }

  protected void formLoggingData(long start, double totalDataCompressedSize) {
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
  }
}
