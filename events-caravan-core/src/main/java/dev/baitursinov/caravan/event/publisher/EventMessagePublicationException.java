package dev.baitursinov.caravan.event.publisher;

import dev.baitursinov.caravan.event.EventReference;

import java.util.List;

public class EventMessagePublicationException extends RuntimeException {

  private final List<EventReference> failedEventReferences;

  public EventMessagePublicationException(List<EventReference> failedEventReferences,
                                          String message) {
    super(message);
    this.failedEventReferences = List.copyOf(failedEventReferences);
  }

  public EventMessagePublicationException(List<EventReference> failedEventReferences,
                                          String message,
                                          Throwable cause) {
    super(message, cause);
    this.failedEventReferences = List.copyOf(failedEventReferences);
  }

  public List<EventReference> failedEventReferences() {
    return failedEventReferences;
  }
}
