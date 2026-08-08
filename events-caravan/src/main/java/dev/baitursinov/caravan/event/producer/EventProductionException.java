package dev.baitursinov.caravan.event.producer;

public class EventProductionException extends RuntimeException {

  public EventProductionException(String message) {
    super(message);
  }

  public EventProductionException(Exception cause) {
    super(cause);
  }
}