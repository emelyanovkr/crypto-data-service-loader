package com.crypto.service.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.*;

public class CompressionHandlerTest {
  CompressionHandler compressionHandler;

  PipedInputStream pin;
  PipedOutputStream pout;

  AtomicBoolean taskRunningStatus = new AtomicBoolean(false);
  AtomicBoolean stopCommand = new AtomicBoolean(false);

  @BeforeEach
  public void initCompressionHandler() {
    pin = new PipedInputStream();
    pout = new PipedOutputStream();
    try {
      pin.connect(pout);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    compressionHandler = new CompressionHandler(pout, taskRunningStatus, stopCommand);
  }

  private static byte[] readBytesFromFile(String filename) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (FileInputStream fis = new FileInputStream(filename)) {
      byte[] fileDataBuffer = new byte[1024];
      int bytesRead;
      while ((bytesRead = fis.read(fileDataBuffer)) != -1) {
        baos.write(fileDataBuffer, 0, bytesRead);
      }
    }
    return baos.toByteArray();
  }

  @Test
  public void compressedDataEqualsToSourceData() {
    List<String> tickersPath = new ArrayList<>();
    String FILENAME_TO_TEST = "src/test/resources/testFilesForCompression/testFile_one.txt";
    tickersPath.add(FILENAME_TO_TEST);

    Thread compressingThread =
        new Thread(() -> compressionHandler.compressFilesWithGZIP(tickersPath));
    compressingThread.start();

    try (GZIPInputStream gzipInputStream = new GZIPInputStream(pin)) {
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      byte[] decompressedData = new byte[1024];

      int bytesRead;
      while ((bytesRead = gzipInputStream.read(decompressedData)) != -1) {
        byteArrayOutputStream.write(decompressedData, 0, bytesRead);
      }

      byte[] decompressedFile = byteArrayOutputStream.toByteArray();
      byte[] fileData = readBytesFromFile(FILENAME_TO_TEST);

      assertArrayEquals(fileData, decompressedFile);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
