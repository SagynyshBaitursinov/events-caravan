package io.saga.caravan.event.sourcing.entity.calculator.snapshotting;

import io.saga.caravan.event.sourcing.EventSourcedRepository;
import io.saga.caravan.event.sourcing.EventSourcingRepositoryContext;
import org.springframework.stereotype.Repository;

import static io.saga.caravan.event.sourcing.entity.calculator.snapshotting.SnapshottingCalculatorEventsConfiguration.SNAPSHOTTING_CALCULATOR;

@Repository
public class SnapshottingCalculatorRepository extends EventSourcedRepository<SnapshottingCalculator> {

  public SnapshottingCalculatorRepository(EventSourcingRepositoryContext context) {
    super(SNAPSHOTTING_CALCULATOR, SnapshottingCalculator.class, context);
  }

  @Override
  protected SnapshottingCalculator createWithBlankState(String entityId) {
    return new SnapshottingCalculator(entityId);
  }
}
