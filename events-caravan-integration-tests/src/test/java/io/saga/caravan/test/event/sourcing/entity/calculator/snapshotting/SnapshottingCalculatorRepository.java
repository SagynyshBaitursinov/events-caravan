package io.saga.caravan.test.event.sourcing.entity.calculator.snapshotting;

import io.saga.caravan.event.sourcing.EventSourcedRepository;
import io.saga.caravan.event.sourcing.EventSourcingRepositoryContext;
import org.springframework.stereotype.Repository;

@Repository
public class SnapshottingCalculatorRepository extends EventSourcedRepository<SnapshottingCalculator> {

  public SnapshottingCalculatorRepository(EventSourcingRepositoryContext context) {
    super(SnapshottingCalculator.class, context);
  }

  @Override
  protected SnapshottingCalculator createWithBlankState(String entityId) {
    return new SnapshottingCalculator(entityId);
  }
}
