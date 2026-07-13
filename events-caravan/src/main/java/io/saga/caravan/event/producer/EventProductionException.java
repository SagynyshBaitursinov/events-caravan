package io.saga.caravan.event.producer;

public class EventProductionException extends RuntimeException {

  public EventProductionException(String message, Exception cause) {
    super(message, cause);
  }

  public EventProductionException(String message) {
    super(message);
  }
}