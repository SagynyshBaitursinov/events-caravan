package dev.baitursinov.caravan.test.event.sourcing.entity.calculator.snapshotting;

import dev.baitursinov.caravan.event.sourcing.EntityName;
import dev.baitursinov.caravan.test.event.sourcing.entity.calculator.Calculator;

import static dev.baitursinov.caravan.test.event.registration.SnapshottingCalculatorEventsConfiguration.SNAPSHOTTING_CALCULATOR;

@EntityName(SNAPSHOTTING_CALCULATOR)
public class SnapshottingCalculator extends Calculator {

  public SnapshottingCalculator(String id) {
    super(id);
  }

  void setCurrentNumber(long currentNumber) {
    super.currentNumber = currentNumber;
  }
}
