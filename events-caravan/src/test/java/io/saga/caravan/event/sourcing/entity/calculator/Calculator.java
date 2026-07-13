package io.saga.caravan.event.sourcing.entity.calculator;

import io.saga.caravan.event.Event;
import io.saga.caravan.event.sourcing.EventSourcedEntity;
import io.saga.caravan.event.sourcing.applying.ApplyEvent;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import static io.saga.caravan.event.sourcing.entity.calculator.CalculatorEventsConfiguration.CALCULATOR;
import static io.saga.caravan.event.sourcing.entity.calculator.CalculatorEventsConfiguration.NUMBER_ADDED;
import static io.saga.caravan.event.sourcing.entity.calculator.CalculatorEventsConfiguration.NUMBER_SUBTRACTED;

@RequiredArgsConstructor
public class Calculator extends EventSourcedEntity {

  final String id;

  @Setter
  @Getter
  long currentNumber = 0;

  public void addNumber(long number) {
    recordEvent(
        NUMBER_ADDED,
        new NumberCarryingPayload(number));
  }

  public void subtractNumber(long number) {
    recordEvent(
        NUMBER_SUBTRACTED,
        new NumberCarryingPayload(number));
  }

  @ApplyEvent(NUMBER_ADDED)
  private void applyAddNumber(Event<NumberCarryingPayload> numberAdded) {
    this.currentNumber += numberAdded.payload().number();
  }

  @ApplyEvent(NUMBER_SUBTRACTED)
  private void applySubtractNumber(Event<NumberCarryingPayload> numberSubtracted) {
    this.currentNumber -= numberSubtracted.payload().number();
  }

  @Override
  public String entityId() {
    return id;
  }

  @Override
  public String entityName() {
    return CALCULATOR;
  }
}
