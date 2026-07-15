package io.saga.caravan.event.sourcing;

public class EventSourcedEntitySetupException extends RuntimeException {

  public EventSourcedEntitySetupException(String message) {
    super(message);
  }
}
