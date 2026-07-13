package io.saga.caravan.event.sourcing.entity.calculator.snapshotting;

import io.saga.caravan.event.sourcing.EventSourcedEntityRepository;
import org.springframework.stereotype.Repository;

import static io.saga.caravan.event.sourcing.entity.calculator.snapshotting.SnapshottingCalculatorEventsConfiguration.SNAPSHOTTING_CALCULATOR;

@Repository
public class SnapshottingCalculatorRepository extends EventSourcedEntityRepository<SnapshottingCalculator> {

  public SnapshottingCalculatorRepository() {
    super(SNAPSHOTTING_CALCULATOR, SnapshottingCalculator.class);
  }

  @Override
  protected SnapshottingCalculator createWithBlankState(String entityId) {
    return new SnapshottingCalculator(entityId);
  }
}
