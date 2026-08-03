package io.saga.caravan.event.sourcing.applying;

public class EventApplyingException extends RuntimeException {

  public EventApplyingException(String message, Throwable cause) {
    super(message, cause);
  }

  public EventApplyingException(String message) {
    super(message);
  }
}
