package io.saga.caravan.event.sourcing.entity.calculator.snapshotting;

import io.saga.caravan.event.sourcing.EntityName;
import io.saga.caravan.event.sourcing.entity.calculator.Calculator;

import static io.saga.caravan.event.sourcing.entity.calculator.snapshotting.SnapshottingCalculatorEventsConfiguration.SNAPSHOTTING_CALCULATOR;

@EntityName(SNAPSHOTTING_CALCULATOR)
public class SnapshottingCalculator extends Calculator {

  public SnapshottingCalculator(String id) {
    super(id);
  }
}
