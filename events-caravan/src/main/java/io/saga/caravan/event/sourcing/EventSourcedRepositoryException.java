package io.saga.caravan.event.sourcing;

public class EventSourcedRepositoryException extends RuntimeException {

  public EventSourcedRepositoryException(String message) {
    super(message);
  }

  public EventSourcedRepositoryException(String message,
                                         Exception cause) {
    super(message, cause);
  }
}
