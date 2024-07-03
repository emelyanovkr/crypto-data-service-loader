package com.crypto.service.flow;

import com.flower.anno.flow.FlowType;
import com.flower.anno.flow.State;
import com.flower.anno.functions.SimpleStepFunction;
import com.flower.anno.params.common.In;
import com.flower.anno.params.common.Out;
import com.flower.anno.params.transit.StepRef;
import com.flower.anno.params.transit.Terminal;
import com.flower.conf.OutPrm;
import com.flower.conf.Transition;

@FlowType(firstStep = "HELLO_STEP")
public class TestFlow {
  @State final String hello = "Hello";
  @State String world;

  @SimpleStepFunction
  static Transition HELLO_STEP(@In String hello,
                               @Out OutPrm<String> world,
                               @StepRef Transition WORLD_STEP,
                               @StepRef Transition HELLO_STEP) {
    System.out.print(hello);
    world.setOutValue(" new_world");
    return WORLD_STEP;
  }

  @SimpleStepFunction
  static Transition WORLD_STEP(@In String world,
                               @Terminal Transition END) {
    System.out.println(world);
    return END;
  }
}
