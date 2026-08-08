package dev.baitursinov.caravan.event.producer;

public class DuplicateEventProductionException extends EventProductionException {

  public DuplicateEventProductionException(String message) {
    super(message);
  }
}