package io.saga.caravan.event.sourcing.entity.calculator;

import io.saga.caravan.event.sourcing.EventSourcedEntityRepository;
import org.springframework.stereotype.Repository;

import static io.saga.caravan.event.sourcing.entity.calculator.CalculatorEventsConfiguration.CALCULATOR;

@Repository
public class CalculatorRepository extends EventSourcedEntityRepository<Calculator> {

  public CalculatorRepository() {
    super(CALCULATOR, Calculator.class);
  }

  @Override
  protected Calculator createWithBlankState(String entityId) {
    return new Calculator(entityId);
  }
}
