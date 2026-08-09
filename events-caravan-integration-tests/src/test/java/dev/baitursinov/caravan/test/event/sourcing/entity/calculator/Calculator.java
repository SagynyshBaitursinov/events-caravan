package dev.baitursinov.caravan.test.event.sourcing.entity.calculator;

import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.sourcing.EntityName;
import dev.baitursinov.caravan.event.sourcing.EventSourcedEntity;
import dev.baitursinov.caravan.event.sourcing.applying.ApplyEvent;
import dev.baitursinov.caravan.test.value.NumberCarryingPayload;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import static dev.baitursinov.caravan.test.event.registration.CalculatorEventsConfiguration.CALCULATOR;
import static dev.baitursinov.caravan.test.event.registration.CalculatorEventsConfiguration.NUMBER_ADDED;
import static dev.baitursinov.caravan.test.event.registration.CalculatorEventsConfiguration.NUMBER_SUBTRACTED;

@EntityName(CALCULATOR)
@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
public class Calculator extends EventSourcedEntity {

  final String id;

  public static Calculator createNew(String id) {
    return new Calculator(id);
  }

  @Getter
  protected long currentNumber = 0;

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
}
