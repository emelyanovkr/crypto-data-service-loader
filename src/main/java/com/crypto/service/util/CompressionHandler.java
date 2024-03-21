package com.crypto.service.util;

import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorOutputStream;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class CompressionHandler implements Runnable {
  private final List<String> ticketsPath;
  private final int BUFFER_SIZE = 16384;

  public CompressionHandler(List<String> ticketsPath) {
    this.ticketsPath = ticketsPath;
  }

  @Override
  public void run() {
    compressFiles();
  }

  public void compressFiles() {
    long start = System.currentTimeMillis();
    System.out.println("Start compressing...");
    for (String file : ticketsPath) {
      try (InputStream in = Files.newInputStream(Paths.get(file));
          OutputStream fout = Files.newOutputStream(Paths.get(file + ".lz4"));
          BufferedOutputStream out = new BufferedOutputStream(fout);
          BlockLZ4CompressorOutputStream lzOut = new BlockLZ4CompressorOutputStream(out)) {
        final byte[] buffer = new byte[BUFFER_SIZE];
        int n;
        while ((n = in.read(buffer)) != -1) {
          lzOut.write(buffer, 0, n);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    System.out.println("Compression finished: " + (System.currentTimeMillis() - start));
  }
}
