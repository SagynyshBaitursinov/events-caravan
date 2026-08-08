package dev.baitursinov.caravan.event.serialization;

public class EventPayloadDeserializationException extends Exception {

  public EventPayloadDeserializationException(String message, Exception exception) {
    super(message, exception);
  }

  public EventPayloadDeserializationException(String message) {
    super(message);
  }
}
