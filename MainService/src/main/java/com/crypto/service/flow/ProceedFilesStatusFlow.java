package com.crypto.service.flow;

import com.crypto.service.config.MainFlowsConfig;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.dao.Tables;
import com.crypto.service.data.TickerFile;
import com.crypto.service.util.FlowsUtil;
import com.flower.anno.flow.FlowType;
import com.flower.anno.flow.State;
import com.flower.anno.functions.SimpleStepFunction;
import com.flower.anno.params.common.In;
import com.flower.anno.params.common.Out;
import com.flower.anno.params.transit.StepRef;
import com.flower.conf.OutPrm;
import com.flower.conf.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

// FLOW 2
@FlowType(firstStep = "RETRIEVE_TICKER_FILES_INFO")
public class ProceedFilesStatusFlow {
  protected static final Logger LOGGER = LoggerFactory.getLogger(ProceedFilesStatusFlow.class);

  protected final MainFlowsConfig mainFlowsConfig;
  protected static int WORK_CYCLE_TIME_SEC;
  protected static int SLEEP_ON_RECONNECT_MS;
  protected static int MAX_RECONNECT_ATTEMPTS;

  protected static final String GET_TICKER_FILES_INFO_ERROR_MSG =
      "CAN'T RETRIEVE TICKER FILES INFO";
  protected static final String UPDATE_FILE_STATUS_ERROR_MSG =
      "CAN'T UPDATE TICKER FILE STATUS ON CHANGES";

  @State protected final ClickHouseDAO clickHouseDAO;
  @State protected List<TickerFile> tickerFiles;

  public ProceedFilesStatusFlow(MainFlowsConfig mainFlowsConfig) {
    this.mainFlowsConfig = mainFlowsConfig;
    WORK_CYCLE_TIME_SEC = mainFlowsConfig.getProceedFilesStatusConfig().getWorkCycleTimeSec();
    SLEEP_ON_RECONNECT_MS = mainFlowsConfig.getProceedFilesStatusConfig().getSleepOnReconnectMs();
    MAX_RECONNECT_ATTEMPTS =
        mainFlowsConfig.getProceedFilesStatusConfig().getMaxReconnectAttempts();

    this.clickHouseDAO = new ClickHouseDAO();
    this.tickerFiles = new ArrayList<>();
  }

  @SimpleStepFunction
  public static Transition RETRIEVE_TICKER_FILES_INFO(
      @In(throwIfNull = true) ClickHouseDAO clickHouseDAO,
      @Out OutPrm<List<TickerFile>> tickerFiles,
      @StepRef Transition PROCEED_FILES_STATUS) {
    List<TickerFile> acquiredTickerFiles =
        FlowsUtil.manageRetryOperation(
            SLEEP_ON_RECONNECT_MS,
            MAX_RECONNECT_ATTEMPTS,
            LOGGER,
            GET_TICKER_FILES_INFO_ERROR_MSG,
            () ->
                clickHouseDAO.selectTickerFilesNamesOnStatus(
                    Tables.TICKER_FILES.getTableName(),
                    TickerFile.FileStatus.DISCOVERED,
                    TickerFile.FileStatus.DOWNLOADING));
    tickerFiles.setOutValue(acquiredTickerFiles);
    return PROCEED_FILES_STATUS;
  }

  @SimpleStepFunction
  public static Transition PROCEED_FILES_STATUS(
      @In ClickHouseDAO clickHouseDAO,
      @In List<TickerFile> tickerFiles,
      @StepRef Transition RETRIEVE_TICKER_FILES_INFO) {
    LocalDate currentDate = LocalDate.now();

    int changesCounter = 0;
    for (TickerFile file : tickerFiles) {
      LocalDate fileDate = file.getCreateDate();
      if (fileDate.isEqual(currentDate) && file.getStatus() == TickerFile.FileStatus.DISCOVERED) {
        file.setStatus(TickerFile.FileStatus.DOWNLOADING);
        ++changesCounter;
      } else if (fileDate.isBefore(currentDate)) {
        file.setStatus(TickerFile.FileStatus.READY_FOR_PROCESSING);
        ++changesCounter;
      }
    }

    int finalChangesCounter = changesCounter;
    FlowsUtil.manageRetryOperation(
        SLEEP_ON_RECONNECT_MS,
        MAX_RECONNECT_ATTEMPTS,
        LOGGER,
        UPDATE_FILE_STATUS_ERROR_MSG,
        () -> {
          if (finalChangesCounter > 0) {
            FlowsUtil.changeTickerFileUpdateStatus(
                clickHouseDAO, tickerFiles, TickerFile.FileStatus.DOWNLOADING);
            FlowsUtil.changeTickerFileUpdateStatus(
                clickHouseDAO, tickerFiles, TickerFile.FileStatus.READY_FOR_PROCESSING);

            LOGGER.info("Processed {} ticker files", finalChangesCounter);
          }
          return null;
        });

    return RETRIEVE_TICKER_FILES_INFO.setDelay(Duration.ofSeconds(WORK_CYCLE_TIME_SEC));
  }
}
