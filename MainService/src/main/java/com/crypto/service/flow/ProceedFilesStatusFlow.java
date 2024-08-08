package com.crypto.service.flow;

import com.clickhouse.client.ClickHouseException;
import com.crypto.service.dao.ClickHouseDAO;
import com.crypto.service.dao.Tables;
import com.crypto.service.data.TickerFile;
import com.crypto.service.util.WorkersUtil;
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

  @State protected final ClickHouseDAO clickHouseDAO;
  @State protected List<TickerFile> tickerFiles;

  public ProceedFilesStatusFlow() {
    this.clickHouseDAO = new ClickHouseDAO();
    this.tickerFiles = new ArrayList<>();
  }

  @SimpleStepFunction
  static Transition RETRIEVE_TICKER_FILES_INFO(
      @In(throwIfNull = true) ClickHouseDAO clickHouseDAO,
      @Out OutPrm<List<TickerFile>> tickerFiles,
      @StepRef Transition PROCEED_FILES_STATUS) {
    try {
      tickerFiles.setOutValue(
          clickHouseDAO.selectTickerFilesNamesOnStatus(
              Tables.TICKER_FILES.getTableName(),
              TickerFile.FileStatus.DISCOVERED,
              TickerFile.FileStatus.DOWNLOADING));
    } catch (ClickHouseException e) {
      LOGGER.error("CAN'T RETRIEVE TICKER FILES INFO - ", e);
      throw new RuntimeException(e);
    }
    return PROCEED_FILES_STATUS;
  }

  @SimpleStepFunction
  static Transition PROCEED_FILES_STATUS(
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

    if (changesCounter > 0) {
      WorkersUtil.changeTickerFileUpdateStatus(
          clickHouseDAO, tickerFiles, TickerFile.FileStatus.DOWNLOADING);
      WorkersUtil.changeTickerFileUpdateStatus(
          clickHouseDAO, tickerFiles, TickerFile.FileStatus.READY_FOR_PROCESSING);

      LOGGER.info("Processed {} ticker files", changesCounter);
    }

    return RETRIEVE_TICKER_FILES_INFO.setDelay(Duration.ofSeconds(15));
  }
}
