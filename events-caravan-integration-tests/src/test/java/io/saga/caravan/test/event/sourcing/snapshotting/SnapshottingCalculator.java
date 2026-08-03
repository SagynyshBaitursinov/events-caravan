package io.saga.caravan.test.event.sourcing.snapshotting;

import io.saga.caravan.event.sourcing.EntityName;
import io.saga.caravan.test.event.sourcing.entity.calculator.Calculator;

import static io.saga.caravan.test.event.sourcing.snapshotting.SnapshottingCalculatorEventsConfiguration.SNAPSHOTTING_CALCULATOR;

@EntityName(SNAPSHOTTING_CALCULATOR)
public class SnapshottingCalculator extends Calculator {

  public SnapshottingCalculator(String id) {
    super(id);
  }
}
