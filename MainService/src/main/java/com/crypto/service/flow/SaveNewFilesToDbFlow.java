package com.crypto.service.flow;

import static java.nio.file.StandardWatchEventKinds.*;

import com.crypto.service.config.MainFlowsConfig;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.dao.Tables;
import com.crypto.service.data.TickerFile;
import com.crypto.service.util.FlowsUtil;
import com.flower.anno.flow.FlowType;
import com.flower.anno.flow.State;
import com.flower.anno.functions.SimpleStepFunction;
import com.flower.anno.params.common.In;
import com.flower.anno.params.common.InOut;
import com.flower.anno.params.common.Out;
import com.flower.anno.params.common.Output;
import com.flower.anno.params.transit.StepRef;
import com.flower.conf.InOutPrm;
import com.flower.conf.NullableInOutPrm;
import com.flower.conf.OutPrm;
import com.flower.conf.Transition;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// FLOW 1
@FlowType(firstStep = "RETRIEVE_FILE_NAMES_LIST_ON_START")
public class SaveNewFilesToDbFlow {
  protected static final Logger LOGGER = LoggerFactory.getLogger(SaveNewFilesToDbFlow.class);
  protected final MainFlowsConfig mainFlowsConfig;
  protected static int FILES_BUFFER_SIZE;
  protected static int DISCOVERY_FILES_TIMEOUT_SEC;
  protected static int SLEEP_ON_RECONNECT_MS;
  protected static int MAX_RECONNECT_ATTEMPTS;

  protected static final String GET_MAX_DATE_ERROR_MSG =
      "CAN'T ACQUIRE MAX DATE FROM CLICKHOUSE DAO";
  protected static final String GET_EXCLUSIVE_TICKER_FILES_ERROR_MSG =
      "CAN'T SELECT EXCLUSIVE TICKER FILES NAMES";

  @State protected static LocalDate CHOSEN_DATE = LocalDate.EPOCH;

  @State protected final ClickHouseDAO clickHouseDAO;
  @State protected final String rootPath;

  @State protected WatchService watcher;
  @State protected Queue<TickerFile> filesBuffer;

  @State protected Long lastFlushTime;

  public SaveNewFilesToDbFlow(MainFlowsConfig mainFlowsConfig, String rootPath) {
    this.mainFlowsConfig = mainFlowsConfig;

    FILES_BUFFER_SIZE = mainFlowsConfig.getDiscoverNewFilesConfig().getFilesBufferSize();
    SLEEP_ON_RECONNECT_MS = mainFlowsConfig.getDiscoverNewFilesConfig().getSleepOnReconnectMs();
    DISCOVERY_FILES_TIMEOUT_SEC =
        mainFlowsConfig.getDiscoverNewFilesConfig().getFlushDiscoveredFilesTimeoutSec();
    MAX_RECONNECT_ATTEMPTS = mainFlowsConfig.getDiscoverNewFilesConfig().getMaxReconnectAttempts();

    this.clickHouseDAO = new ClickHouseDAO();
    this.rootPath = rootPath;
    this.filesBuffer = new LinkedList<>();
    lastFlushTime = System.currentTimeMillis();
  }

  @SimpleStepFunction
  public static Transition RETRIEVE_FILE_NAMES_LIST_ON_START(
      @In ClickHouseDAO clickHouseDAO,
      @In String rootPath,
      @Out OutPrm<Queue<TickerFile>> filesBuffer,
      @StepRef Transition INIT_DIRECTORY_WATCHER_SERVICE) {
    List<TickerFile> fileNames = new ArrayList<>();

    FlowsUtil.manageRetryOperation(
        SLEEP_ON_RECONNECT_MS,
        MAX_RECONNECT_ATTEMPTS,
        LOGGER,
        GET_MAX_DATE_ERROR_MSG,
        () -> {
          LocalDate currentDate = LocalDate.now();
          LocalDate maxDate =
              clickHouseDAO.selectMaxTickerFilesDate(
                  "create_date", Tables.TICKER_FILES.getTableName());

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
          return null;
        });
    filesBuffer.setOutValue(new ConcurrentLinkedQueue<>(fileNames));

    return INIT_DIRECTORY_WATCHER_SERVICE;
  }

  @SimpleStepFunction
  public static Transition INIT_DIRECTORY_WATCHER_SERVICE(
      @In String rootPath,
      @InOut(throwIfNull = true, out = Output.OPTIONAL) InOutPrm<LocalDate> CHOSEN_DATE,
      @InOut(throwIfNull = true) InOutPrm<WatchService> watcher,
      @StepRef Transition INIT_DIRECTORY_WATCHER_SERVICE,
      @StepRef Transition GET_DIRECTORY_WATCHER_EVENTS_AND_ADD_TO_BUFFER) {

    WatchService watcherService = watcher.getInValue();
    if (watcherService != null) {
      try {
        watcherService.close();
        LOGGER.info("Watcher service closed successfully. Reinit watcher.");
      } catch (IOException e) {
        LOGGER.error("CAN'T CLOSE WATCHER SERVICE - ", e);
        LOGGER.info("Attempt to reinit watcher service");
        return INIT_DIRECTORY_WATCHER_SERVICE.setDelay(Duration.ofMillis(SLEEP_ON_RECONNECT_MS));
      }
    }
    try {
      watcherService = FileSystems.getDefault().newWatchService();
      LocalDate chosenDateLocal = LocalDate.now();
      Path watchedDirectory = Path.of(rootPath + "/" + chosenDateLocal);

      CHOSEN_DATE.setOutValue(chosenDateLocal);
      if (!Files.exists(watchedDirectory)) {
        Optional<Path> lastDirectory =
            Files.list(Path.of(rootPath)).filter(Files::isDirectory).max(Comparator.naturalOrder());
        if (lastDirectory.isPresent()) {
          LOGGER.warn(
              "ERROR ACQUIRING TODAY'S DIRECTORY - {}, LAST DATE DIR IS TAKEN - {}",
              LocalDate.now(),
              lastDirectory.get().getFileName());
          CHOSEN_DATE.setOutValue(LocalDate.parse(lastDirectory.get().getFileName().toString()));
          watchedDirectory = lastDirectory.get();
        } else {
          LOGGER.error("NO SUITABLE DIRECTORY FOUND FOR WATCHER SERVICE, EXIT.");
          throw new RuntimeException("NO SUITABLE DIRECTORY FOUND FOR WATCHER SERVICE");
        }
      }

      watchedDirectory.register(watcherService, ENTRY_CREATE);
      LOGGER.info("Directory watcher started - {}", watchedDirectory);

      watcher.setOutValue(watcherService);
    } catch (IOException e) {
      LOGGER.error("CAN'T SETUP WATCHER SERVICE - ", e);
      throw new RuntimeException(e);
    }
    return GET_DIRECTORY_WATCHER_EVENTS_AND_ADD_TO_BUFFER;
  }

  @SimpleStepFunction
  public static Transition GET_DIRECTORY_WATCHER_EVENTS_AND_ADD_TO_BUFFER(
      @In WatchService watcher,
      @In Queue<TickerFile> filesBuffer,
      @StepRef Transition TRY_TO_FLUSH_BUFFER) {
    WatchKey key;
    while ((key = watcher.poll()) != null) {
      for (WatchEvent<?> event : key.pollEvents()) {
        WatchEvent.Kind<?> kind = event.kind();

        if (kind == ENTRY_CREATE) {
          filesBuffer.add(new TickerFile(event.context().toString(), CHOSEN_DATE, null));
        }
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
      @StepRef Transition POST_FLUSH) {
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

      FlowsUtil.manageRetryOperation(
          SLEEP_ON_RECONNECT_MS,
          MAX_RECONNECT_ATTEMPTS,
          LOGGER,
          GET_EXCLUSIVE_TICKER_FILES_ERROR_MSG,
          () -> {
            List<String> filesFromDatabase =
                clickHouseDAO.selectExclusiveTickerFilesNames(
                    TickerFile.getSQLFileNames(localTickerFiles),
                    Tables.TICKER_FILES.getTableName());

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

            if (localTickerFiles.isEmpty()) {
              return null;
            }

            clickHouseDAO.insertTickerFilesInfo(
                TickerFile.formDataToInsert(localTickerFiles), Tables.TICKER_FILES.getTableName());

            LOGGER.info("Saved {} ticker files info", localTickerFiles.size());

            return null;
          });
    }
    return POST_FLUSH;
  }

  @SimpleStepFunction
  public static Transition POST_FLUSH(
      @In(throwIfNull = true) LocalDate CHOSEN_DATE,
      @StepRef Transition INIT_DIRECTORY_WATCHER_SERVICE,
      @StepRef Transition GET_DIRECTORY_WATCHER_EVENTS_AND_ADD_TO_BUFFER) {
    LocalDate currentDate = LocalDate.now();

    if (currentDate.getDayOfMonth() != CHOSEN_DATE.getDayOfMonth()) {
      LOGGER.info(
          "FLOW - DAY PASSED ({} - {}) -> REINIT WATCHER SERVICE IN: {} SEC.",
          currentDate,
          CHOSEN_DATE,
          DISCOVERY_FILES_TIMEOUT_SEC);
      return INIT_DIRECTORY_WATCHER_SERVICE.setDelay(
          Duration.ofSeconds(DISCOVERY_FILES_TIMEOUT_SEC));
    }

    return GET_DIRECTORY_WATCHER_EVENTS_AND_ADD_TO_BUFFER.setDelay(
        Duration.ofSeconds(DISCOVERY_FILES_TIMEOUT_SEC));
  }
}
