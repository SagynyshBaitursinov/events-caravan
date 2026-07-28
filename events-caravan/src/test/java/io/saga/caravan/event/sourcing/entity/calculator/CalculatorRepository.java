package io.saga.caravan.event.sourcing.entity.calculator;

import io.saga.caravan.event.sourcing.EventSourcedRepository;
import io.saga.caravan.event.sourcing.EventSourcingRepositoryContext;
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
