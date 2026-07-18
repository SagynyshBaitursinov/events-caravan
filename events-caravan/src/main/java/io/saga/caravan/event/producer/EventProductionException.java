package io.saga.caravan.event.producer;

public class EventProductionException extends RuntimeException {

  public EventProductionException(String message) {
    super(message);
  }
}