package io.saga.caravan.event.sourcing.entity.calculator.snapshotting;

import io.saga.caravan.entity.EntityReference;
import io.saga.caravan.event.sourcing.snapshot.SnapshotTaker;
import org.springframework.stereotype.Component;

@Component
public class CalculatorSnapshotTaker extends SnapshotTaker<SnapshottingCalculator, CalculatorSnapshot> {

  public static final int FREQUENCY_OF_SNAPSHOTS = 5;

  public CalculatorSnapshotTaker() {
    super(SnapshottingCalculator.class, CalculatorSnapshot.class);
  }

  @Override
  public CalculatorSnapshot takeSnapshot(SnapshottingCalculator entity) {
    return new CalculatorSnapshot(entity.getCurrentNumber());
  }

  @Override
  public SnapshottingCalculator recreateFromSnapshot(EntityReference entityReference,
                                                     CalculatorSnapshot snapshotPayload) {
    var result = new SnapshottingCalculator(entityReference.entityId());
    result.setCurrentNumber(snapshotPayload.currentNumber());
    return result;
  }

  @Override
  public int frequencyOfSnapshots() {
    return FREQUENCY_OF_SNAPSHOTS;
  }
}
