package io.saga.caravan.event.sourcing.entity.calculator.snapshotting;

import io.saga.caravan.event.sourcing.entity.calculator.Calculator;

import static io.saga.caravan.event.sourcing.entity.calculator.snapshotting.SnapshottingCalculatorEventsConfiguration.SNAPSHOTTING_CALCULATOR;

public class SnapshottingCalculator extends Calculator {

  public SnapshottingCalculator(String id) {
    super(id);
  }

  @Override
  public String entityName() {
    return SNAPSHOTTING_CALCULATOR;
  }
}
