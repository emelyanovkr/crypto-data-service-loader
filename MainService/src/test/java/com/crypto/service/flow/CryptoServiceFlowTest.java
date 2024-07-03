package com.crypto.service.flow;

import com.flower.conf.FlowExec;
import com.flower.conf.FlowFuture;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import com.flower.engine.Flower;

import java.util.concurrent.ExecutionException;

public class CryptoServiceFlowTest {

  @Test
  public void testCryptoServiceFlow() throws ExecutionException, InterruptedException {
    Flower flower = new Flower();
    flower.registerFlow(CryptoServiceFlow.class);
    flower.initialize();

    FlowExec<CryptoServiceFlow> flowExec = flower.getFlowExec(CryptoServiceFlow.class);

    CryptoServiceFlow testFlow = new CryptoServiceFlow(100, "212", "2121", 21);
    FlowFuture<CryptoServiceFlow> flowFuture = flowExec.runFlow(testFlow);
    CryptoServiceFlow testFlow2 = flowFuture.getFuture().get();
    Assertions.assertEquals(testFlow, testFlow2);
  }

  @Test
  public void testTestFlow() throws ExecutionException, InterruptedException {
    Flower flower = new Flower();
    flower.registerFlow(TestFlow.class);
    flower.initialize();

    FlowExec<TestFlow> flowExec = flower.getFlowExec(TestFlow.class);
    TestFlow testFlow = new TestFlow();
    FlowFuture<TestFlow> flowFuture = flowExec.runFlow(testFlow);
    TestFlow testFlow2 = flowFuture.getFuture().get();
    Assertions.assertEquals(testFlow, testFlow2);
  }
}
