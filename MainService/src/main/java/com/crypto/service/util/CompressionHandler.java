package com.crypto.service.util;

import com.crypto.service.MainApplication;
import com.crypto.service.config.TickersDataConfig;
import com.crypto.service.data.TickerFile;
import java.io.*;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class CompressionHandler {
  protected final int BUFFER_SIZE;
  protected final int EXPECTED_COLUMNS;
  protected final PipedOutputStream pout;
  protected final Logger LOGGER = LoggerFactory.getLogger(CompressionHandler.class);

  protected final AtomicBoolean taskRunningStatus;
  protected final AtomicBoolean stopCommand;

  protected CompressionHandler(
      PipedOutputStream pout, AtomicBoolean taskRunningStatus, AtomicBoolean stopCommand) {
    this.pout = pout;
    this.taskRunningStatus = taskRunningStatus;
    this.stopCommand = stopCommand;

    TickersDataConfig tickersDataConfig = MainApplication.applicationConfig.getTickersDataConfig();
    BUFFER_SIZE = tickersDataConfig.getCompressionHandlerConfig().getCompressionBufferSize();
    EXPECTED_COLUMNS = tickersDataConfig.getCompressionHandlerConfig().getValidExpectedColumns();
  }

  public static CompressionHandler createCompressionHandler(
      PipedOutputStream pout, AtomicBoolean taskRunningStatus, AtomicBoolean stopCommand) {
    return new CompressionHandler(pout, taskRunningStatus, stopCommand);
  }

  private boolean checkLineValid(String line) {
    String[] columns = line.split(",");
    return columns.length == EXPECTED_COLUMNS;
  }

  public void compressFilesWithGZIP(List<Path> tickersPath, HashSet<TickerFile> tickerFiles) {
    if (tickersPath == null || tickersPath.isEmpty()) {
      return;
    }

    try {
      long start = System.currentTimeMillis();
      double totalDataCompressedSize = 0;

      try (GZIPOutputStream gzOut = new GZIPOutputStream(pout)) {
        StringBuilder buffer = new StringBuilder(BUFFER_SIZE);

        for (Path filePath : tickersPath) {
          String fileName = filePath.getFileName().toString();

          try (BufferedReader bufferedReader =
              new BufferedReader(new FileReader(filePath.toFile()))) {
            String line;

            while ((line = bufferedReader.readLine()) != null) {
              if (stopCommand.get()) {
                return;
              }

              if (checkLineValid(line)) {
                buffer.append(line).append("\n");

                if (buffer.length() >= BUFFER_SIZE) {
                  byte[] data = buffer.toString().getBytes();
                  gzOut.write(data, 0, data.length);
                  totalDataCompressedSize += data.length;
                  buffer.setLength(0);
                }
              } else {
                LOGGER.warn("IN FILE - {} - SKIPPING BAD LINE - {}", fileName, line);
              }
            }
          }
          if (!buffer.isEmpty()) {
            gzOut.write(buffer.toString().getBytes());
            totalDataCompressedSize += buffer.length();
            buffer.setLength(0);
          }

          Optional<TickerFile> representingFile =
              tickerFiles.stream()
                  .filter(tickerFile -> tickerFile.getFileName().equals(fileName))
                  .findFirst();
          if (representingFile.isPresent()) {
            representingFile.get().setStatus(TickerFile.FileStatus.FINISHED);
          } else {
            LOGGER.error("REPRESENTING FILE IS MISSING - {}", fileName);
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
