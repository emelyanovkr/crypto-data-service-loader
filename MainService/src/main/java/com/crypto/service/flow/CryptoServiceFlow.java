package com.crypto.service.flow;

import com.crypto.service.data.TickerFile;
import com.crypto.service.data.TickersDataLoader;
import com.flower.anno.flow.FlowType;
import com.flower.anno.flow.State;
import com.flower.anno.functions.SimpleStepFunction;
import com.flower.anno.params.common.In;
import com.flower.anno.params.step.FlowFactory;
import com.flower.anno.params.transit.StepRef;
import com.flower.anno.params.transit.Terminal;
import com.flower.conf.FlowFactoryPrm;
import com.flower.conf.Transition;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.flower.conf.FlowFactoryPrm;

import java.io.FileWriter;
import java.io.File;
import java.time.Duration;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@FlowType(firstStep = "INIT_STEP")
public class CryptoServiceFlow {
  protected static final Logger LOGGER = LoggerFactory.getLogger(CryptoServiceFlow.class);

  // Helper objects and functions

  protected static final DateTimeFormatter DATE_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  protected static String getCurrentDate() {
    LocalDateTime currentDate = LocalDateTime.now();
    return currentDate.format(DATE_FORMATTER);
  }

  protected static String getPreviousPeriodDate(long periodSeconds) {
    LocalDateTime previousPeriodDate = LocalDateTime.now().minusSeconds(periodSeconds + 1);
    return previousPeriodDate.format(DATE_FORMATTER);
  }

  protected static LocalDateTime toDate(String dateTimeString) {
    return LocalDateTime.parse(dateTimeString, DATE_FORMATTER);
  }

  protected static List<String> getFilesInDirectory(String dataPath) {
    String currentSourcePath = dataPath + "/" + getCurrentDate();
    File searchDirectory = new File(currentSourcePath);

    List<String> tickerNames;
    try {
      tickerNames = List.of(Objects.requireNonNull(searchDirectory.list()));
    } catch (Exception e) {
      LOGGER.error("FAILED TO SEARCH DIRECTORY - ", e);
      throw new RuntimeException();
    }

    // TODO: planned to use later, remove if not needed
    // List<TickerFile> tickerFiles = fillTickerFileList(tickerNames);

    return List.copyOf(tickerNames).stream()
        .map(fileName -> Paths.get(currentSourcePath, fileName).toString())
        .collect(Collectors.toList());
  }

/*  protected static List<TickerFile> fillTickerFileList(List<String> tickerNames) {
    return List.copyOf(tickerNames).stream()
        .map(fileName -> new TickerFile(fileName, TickerFile.FileStatus.NOT_LOADED))
        .collect(Collectors.toList());
  }*/

  // ------------------------- Flow -------------------------

  @State final long periodSeconds;
  @State final String lastDateFilePath;
  @State final String dataPath;
  @State final int uploadPartsCount;

  public CryptoServiceFlow(
      long periodSeconds, String lastDateFilePath, String dataPath, int uploadPartsCount) {
    this.periodSeconds = periodSeconds;
    this.lastDateFilePath = lastDateFilePath;
    this.dataPath = dataPath;
    this.uploadPartsCount = uploadPartsCount;
  }

  @SimpleStepFunction
  static Transition INIT_STEP(
      @In long periodSeconds,
      @In String lastDateFilePath,
      @StepRef Transition UPLOAD_STEP,
      @StepRef Transition INIT_STEP,
      @Terminal Transition END)
      throws IOException {
    File lastDateFile = new File(lastDateFilePath);

    if (!lastDateFile.createNewFile()) {
      try (FileWriter fileWriter = new FileWriter(lastDateFile, false)) { // overwrite mode
        fileWriter.write(getPreviousPeriodDate(periodSeconds));
        System.out.println("File written successfully");
      }
    }

    LocalDateTime previousPeriodDate = toDate(Files.readString(lastDateFile.toPath()));
    Duration duration = Duration.between(LocalDateTime.now(), previousPeriodDate);
    long secondsDifference = duration.getSeconds();

    if (secondsDifference > periodSeconds) {
      // Perform upload
      return UPLOAD_STEP;
    } else {
      // Sleep-wait until the end of this period
      return INIT_STEP.setDelay(duration);
    }
  }

  @SimpleStepFunction
  static Transition UPLOAD_STEP(
      @In String lastDateFilePath,
      @In String dataPath,
      @In int uploadPartsCount,
      @FlowFactory FlowFactoryPrm<UploadFilesFlow> uploadFilesFlowFactory,
      @StepRef Transition INIT_STEP,
      @Terminal Transition END)
      throws IOException {
    List<String> tickerAbsolutePaths = getFilesInDirectory(dataPath);

    List<List<String>> tickerParts =
        Lists.partition(
            tickerAbsolutePaths,
            tickerAbsolutePaths.size() < (uploadPartsCount)
                ? 1
                : (int) Math.ceil((double) tickerAbsolutePaths.size() / uploadPartsCount));

    for (List<String> tickerPartition : tickerParts) {
      // TODO: Run child upload files flows
      // uploadFilesFlowFactory.runChildFlow();
    }

    return END;
  }
}
