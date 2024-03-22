package com.crypto.service.util;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.zip.GZIPOutputStream;

public class CompressionHandler implements Runnable {
  private final List<String> ticketsPath;
  private final int BUFFER_SIZE = 32768;

  public CompressionHandler(List<String> ticketsPath) {
    this.ticketsPath = ticketsPath;
  }

  @Override
  public void run() {
    compressFilesWithGZIP();
  }

  /*public void compressFilesWithApache() {
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
  }*/

  public void compressFilesWithGZIP() {
    for (String file : ticketsPath) {
      try (InputStream in = new FileInputStream(file);
          OutputStream fout = Files.newOutputStream(Paths.get(file + ".gz"));
          BufferedOutputStream out = new BufferedOutputStream(fout);
          GZIPOutputStream gzOut = new GZIPOutputStream(out)) {
        final byte[] buffer = new byte[BUFFER_SIZE];
        int n;
        while ((n = in.read(buffer)) != -1) {
          gzOut.write(buffer, 0, n);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
