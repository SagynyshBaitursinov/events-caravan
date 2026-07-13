package io.saga.caravan.event.sourcing;

public class EventStoreException extends RuntimeException {

  public EventStoreException(String message, Exception cause) {
    super(message, cause);
  }
}
