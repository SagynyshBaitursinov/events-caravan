package dev.baitursinov.caravan.event.consumer.handler;

public class EventHandlerSetupException extends RuntimeException {

  public EventHandlerSetupException(String message) {
    super(message);
  }

  public EventHandlerSetupException(String message, Exception cause) {
    super(message, cause);
  }
}
