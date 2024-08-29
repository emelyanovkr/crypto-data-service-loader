package com.crypto.service.flow;

import com.crypto.service.MainApplication;
import com.crypto.service.config.MainFlowsConfig;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.data.TickerFile;
import com.flower.anno.flow.FlowType;
import com.flower.anno.flow.State;

import java.util.*;
import java.time.Duration;
import com.clickhouse.client.ClickHouseException;
import com.crypto.service.dao.Tables;
import com.flower.anno.functions.SimpleStepFunction;
import com.flower.anno.params.common.In;
import com.flower.anno.params.common.InOut;
import com.flower.anno.params.common.Out;
import com.flower.anno.params.common.Output;
import com.flower.anno.params.transit.StepRef;
import com.flower.conf.NullableInOutPrm;
import com.flower.conf.OutPrm;
import com.flower.conf.Transition;
import com.flower.conf.InOutPrm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.nio.file.*;
import static java.nio.file.StandardWatchEventKinds.*;

import java.util.concurrent.ConcurrentLinkedQueue;

// FLOW 1
@FlowType(firstStep = "RETRIEVE_FILE_NAMES_LIST_ON_START")
public class SaveNewFilesToDbFlow {
  protected static final Logger LOGGER = LoggerFactory.getLogger(SaveNewFilesToDbFlow.class);
  private static WatchService watcherService;

  protected final MainFlowsConfig mainFlowsConfig;
  protected static int FILES_BUFFER_SIZE;
  protected static int DISCOVERY_FILES_TIMEOUT_SEC;

  @State protected final ClickHouseDAO clickHouseDAO;
  @State protected final String rootPath;

  @State protected WatchService watcher;
  @State protected Queue<TickerFile> filesBuffer;

  @State protected Long lastFlushTime;

  public SaveNewFilesToDbFlow(MainFlowsConfig mainFlowsConfig, String rootPath) {
    this.mainFlowsConfig = mainFlowsConfig;
    FILES_BUFFER_SIZE = mainFlowsConfig.getDiscoverNewFilesConfig().getFilesBufferSize();
    DISCOVERY_FILES_TIMEOUT_SEC =
        mainFlowsConfig.getDiscoverNewFilesConfig().getFlushDiscoveredFilesTimeoutSec();

    this.clickHouseDAO = new ClickHouseDAO();
    this.rootPath = rootPath;
    this.filesBuffer = new LinkedList<>();
    lastFlushTime = System.currentTimeMillis();
  }

  /**
   * This step will run once on flow start and scan the list of files that could've been created
   * before start
   */
  @SimpleStepFunction
  public static Transition RETRIEVE_FILE_NAMES_LIST_ON_START(
      @In ClickHouseDAO clickHouseDAO,
      @In String rootPath,
      @Out OutPrm<Queue<TickerFile>> filesBuffer,
      @StepRef Transition INIT_DIRECTORY_WATCHER_SERVICE) {
    List<TickerFile> fileNames = new ArrayList<>();
    try {
      LocalDate currentDate = LocalDate.now();
      LocalDate maxDate =
          clickHouseDAO.selectMaxTickerFilesDate("create_date", Tables.TICKER_FILES.getTableName());

      LOGGER.info("Retrieved date of last uploaded tickerFiles - {}", maxDate);

      while (maxDate.isBefore(currentDate)) {
        Path dateDir = Paths.get(rootPath + "/" + maxDate);
        if (Files.exists(dateDir)) {
          try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(dateDir)) {
            for (Path entry : directoryStream) {
              if (Files.isRegularFile(entry)) {
                fileNames.add(new TickerFile(entry.getFileName().toString(), maxDate, null));
              }
            }
          } catch (IOException e) {
            LOGGER.error("CAN'T OPEN DIRECTORY STREAM (SAVE FILES) - ", e);
            throw new RuntimeException(e);
          }
        }
        maxDate = maxDate.plusDays(1);
      }
    } catch (ClickHouseException e) {
      LOGGER.error("CAN'T ACQUIRE MAX DATE - ", e);
      throw new RuntimeException(e);
    }

    filesBuffer.setOutValue(new ConcurrentLinkedQueue<>(fileNames));

    return INIT_DIRECTORY_WATCHER_SERVICE;
  }

  @SimpleStepFunction
  public static Transition INIT_DIRECTORY_WATCHER_SERVICE(
      @In String rootPath,
      @Out OutPrm<WatchService> watcher,
      @StepRef Transition GET_DIRECTORY_WATCHER_EVENTS_AND_ADD_TO_BUFFER)
      throws IOException {

    // TODO: close previous watcher
    // watchService.close();

    WatchService watcherService = FileSystems.getDefault().newWatchService();
    Path watchedDirectory = Path.of(rootPath + "/" + LocalDate.now());
    watchedDirectory.register(watcherService, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);

    LOGGER.info("Directory watcher started - {}", watchedDirectory);

    watcher.setOutValue(watcherService);

    return GET_DIRECTORY_WATCHER_EVENTS_AND_ADD_TO_BUFFER;
  }

  @SimpleStepFunction
  public static Transition GET_DIRECTORY_WATCHER_EVENTS_AND_ADD_TO_BUFFER(
      @In WatchService watcher,
      @In Queue<TickerFile> filesBuffer,
      @StepRef Transition TRY_TO_FLUSH_BUFFER)
      throws IOException {
    WatchKey key;
    while ((key = watcher.poll()) != null) {
      for (WatchEvent<?> event : key.pollEvents()) {
        WatchEvent.Kind<?> kind = event.kind();

        if (kind == ENTRY_CREATE) {
          filesBuffer.add(new TickerFile(event.context().toString(), LocalDate.now(), null));
        }
        /*
        else if (kind == ENTRY_DELETE) {
          System.out.println("ENTRY DELETED: " + event.context().toString());
        } else if (kind == ENTRY_MODIFY) {
          System.out.println("ENTRY MODIFIED: " + event.context().toString());
        }
        */
      }

      boolean valid = key.reset();
      if (!valid) {
        break;
      }
    }
    return TRY_TO_FLUSH_BUFFER;
  }

  @SimpleStepFunction
  public static Transition TRY_TO_FLUSH_BUFFER(
      @In ClickHouseDAO clickHouseDAO,
      @InOut(out = Output.OPTIONAL) NullableInOutPrm<Long> lastFlushTime,
      @InOut(out = Output.OPTIONAL) InOutPrm<Queue<TickerFile>> filesBuffer,
      @StepRef Transition POST_FLUSH)
      throws ClickHouseException {
    boolean fileBufferSizeSufficient = filesBuffer.getInValue().size() > FILES_BUFFER_SIZE;
    boolean fileTimeoutElapsed =
        System.currentTimeMillis() - lastFlushTime.getInValue()
            > DISCOVERY_FILES_TIMEOUT_SEC * 1000L;
    boolean flushRequired = fileTimeoutElapsed || fileBufferSizeSufficient;

    if (flushRequired) {
      if (filesBuffer.getInValue().isEmpty()) {
        return POST_FLUSH;
      }

      Queue<TickerFile> localTickerFiles = filesBuffer.getInValue();
      filesBuffer.setOutValue(new LinkedList<>());

      lastFlushTime.setOutValue(System.currentTimeMillis());

      List<String> filesFromDatabase =
          clickHouseDAO.selectExclusiveTickerFilesNames(
              TickerFile.getSQLFileNames(localTickerFiles), Tables.TICKER_FILES.getTableName());

      Set<String> filesInDatabase = new HashSet<>(filesFromDatabase);
      for (Iterator<TickerFile> localIterator = localTickerFiles.iterator();
          localIterator.hasNext(); ) {
        TickerFile localTickerFile = localIterator.next();
        if (!filesInDatabase.contains(localTickerFile.getFileName())) {
          localTickerFile.setStatus(TickerFile.FileStatus.DISCOVERED);
        } else {
          localIterator.remove();
        }
      }

      clickHouseDAO.insertTickerFilesInfo(
          TickerFile.formDataToInsert(localTickerFiles), Tables.TICKER_FILES.getTableName());

      LOGGER.info("Saved {} ticker files info", localTickerFiles.size());
    }
    return POST_FLUSH;
  }

  @SimpleStepFunction
  public static Transition POST_FLUSH(
      @StepRef Transition INIT_DIRECTORY_WATCHER_SERVICE,
      @StepRef Transition GET_DIRECTORY_WATCHER_EVENTS_AND_ADD_TO_BUFFER) {
    // TODO: Determine if we need to reinit watcher
    // return
    // INIT_DIRECTORY_WATCHER_SERVICE.setDelay(Duration.ofSeconds(DISCOVERY_FILES_TIMEOUT_SEC));
    return GET_DIRECTORY_WATCHER_EVENTS_AND_ADD_TO_BUFFER.setDelay(
        Duration.ofSeconds(DISCOVERY_FILES_TIMEOUT_SEC));
  }
}
