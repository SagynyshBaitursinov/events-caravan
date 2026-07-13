package io.saga.caravan.event.consumer.messaging;

import io.saga.caravan.event.Event;

public class EventMessageConsumptionException extends RuntimeException {

  public static final String MESSAGE = "Event message consumption has failed for %s";

  public EventMessageConsumptionException(Event<?> event, Throwable cause) {
    super(
        MESSAGE.formatted(event),
        cause);
  }

  public EventMessageConsumptionException(Throwable cause) {
    super(cause);
  }
}
