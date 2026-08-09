package dev.baitursinov.caravan.test.event.handler;

import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.EventType;
import dev.baitursinov.caravan.event.consumer.handler.EventHandler;
import dev.baitursinov.caravan.test.service.AdditionService;
import dev.baitursinov.caravan.test.value.NumberAdditionRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import static dev.baitursinov.caravan.test.event.registration.NumberAdditionEventConfiguration.NUMBER_ADDITION_REQUEST;
import static dev.baitursinov.caravan.test.event.registration.NumberAdditionEventConfiguration.RECEIVED;

@Component
@RequiredArgsConstructor
public class NumberAdditionRequestHandler implements EventHandler<NumberAdditionRequest> {

  public static final EventType INTERESTED_EVENT_TYPE = new EventType(NUMBER_ADDITION_REQUEST, RECEIVED);

  private final AdditionService additionService;

  @Override
  public boolean isOfInterest(Event<NumberAdditionRequest> event) {
    return event.eventType().equals(INTERESTED_EVENT_TYPE);
  }

  @Override
  public void handle(Event<NumberAdditionRequest> event) {
    additionService.addToCalculator(
        event.payload().calculatorId(),
        event.payload().number());
  }
}
