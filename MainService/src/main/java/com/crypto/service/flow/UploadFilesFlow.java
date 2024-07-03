package com.crypto.service.flow;

import com.crypto.service.dao.Tables;
import com.crypto.service.data.TickersDataLoader;
import com.crypto.service.util.CompressionHandler;
import com.flower.anno.flow.FlowType;
import com.flower.anno.flow.State;
import com.flower.anno.functions.SimpleStepFunction;
import com.flower.anno.params.common.In;
import com.flower.anno.params.transit.StepRef;
import com.flower.anno.params.transit.Terminal;
import com.flower.conf.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@FlowType(firstStep = "INIT_STEP")
public class UploadFilesFlow {
  protected final static Logger LOGGER = LoggerFactory.getLogger(UploadFilesFlow.class);

  @State final List<String> tickerPartition;
  @State final int maxFlushAttempts;

  public UploadFilesFlow(List<String> tickerPartition, int maxFlushAttempts) {
    this.tickerPartition = tickerPartition;
    this.maxFlushAttempts = maxFlushAttempts;
  }

  @SimpleStepFunction
  static Transition INIT_STEP(
    List<String> tickerPartition,
    @Terminal Transition END) {
      //new TickersInsertTask(tickerPartition)
      return END;
    }

  /*protected class TickersInsertTask implements Runnable {
    protected final List<String> tickerPartition;
    protected CompressionHandler compressionHandler;

    public TickersInsertTask(List<String> tickerPartition) {
      this.tickerPartition = tickerPartition;
    }

    @Override
    public void run() {
      startInsertTickers();
    }

    protected void startInsertTickers() {
      AtomicBoolean compressionTaskRunning = new AtomicBoolean(false);
      AtomicBoolean insertSuccessful = new AtomicBoolean(false);
      for (int i = 0; i < MAX_FLUSH_ATTEMPTS; i++) {
        AtomicBoolean stopCompressionCommand = new AtomicBoolean(false);

        PipedOutputStream pout = new PipedOutputStream();
        PipedInputStream pin = new PipedInputStream();

        try {
          pin.connect(pout);

          // spinning lock
          while (compressionTaskRunning.get()) {}

          compressionTaskRunning.set(true);

          compressionHandler =
              CompressionHandler.createCompressionHandler(
                  pout, compressionTaskRunning, stopCompressionCommand);

          compression_executor.execute(
              () -> compressionHandler.compressFilesWithGZIP(tickerPartition));

          clickHouseDAO.insertTickersData(pin, Tables.TICKERS_DATA.getTableName());
          insertSuccessful.set(true);
          break;
        } catch (Exception e) {
          LOGGER.error("FAILED TO INSERT TICKERS DATA - ", e);

          stopCompressionCommand.set(true);

          try {
            pin.close();
            Thread.sleep(500);
          } catch (InterruptedException | IOException ex) {
            throw new RuntimeException(ex);
          }
        }
      }
      if (!insertSuccessful.get()) {
        System.err.println(this.getClass().getName() + " LOST TICKERS: ");
      }
    }
  }*/
}
