package io.saga.caravan.event.serialization;

public class EventDeserializationException extends Exception {

  public EventDeserializationException(String message, Exception exception) {
    super(message, exception);
  }

  public EventDeserializationException(String message) {
    super(message);
  }
}
