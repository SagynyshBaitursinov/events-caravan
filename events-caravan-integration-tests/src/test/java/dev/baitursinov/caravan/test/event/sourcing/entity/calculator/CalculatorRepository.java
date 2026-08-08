package dev.baitursinov.caravan.test.event.sourcing.entity.calculator;

import dev.baitursinov.caravan.event.sourcing.EventSourcedRepository;
import dev.baitursinov.caravan.event.sourcing.EventSourcingRepositoryContext;
import org.springframework.stereotype.Repository;

@Repository
public class CalculatorRepository extends EventSourcedRepository<Calculator> {

  public CalculatorRepository(EventSourcingRepositoryContext context) {
    super(Calculator.class, context);
  }

  @Override
  protected Calculator createWithBlankState(String entityId) {
    return new Calculator(entityId);
  }
}
