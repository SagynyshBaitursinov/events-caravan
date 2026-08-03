package io.saga.caravan.event;

import io.saga.caravan.entity.EntityReference;

import static io.saga.caravan.utils.TextUtils.hasText;
import static java.util.Objects.requireNonNull;

/**
 * Identifies a single {@link Event}, without its payload: the {@code sequenceNumber}-th event
 * named {@code eventName} recorded against {@code entityReference}.
 */
public record EventReference(EntityReference entityReference,
                             long sequenceNumber,
                             String eventName) {

  public EventReference {
    requireNonNull(entityReference, "entityReference cannot be null");

    if (sequenceNumber <= 0) {
      throw new IllegalArgumentException("sequenceNumber must be positive number");
    }

    if (!hasText(eventName)) {
      throw new IllegalArgumentException(
          "sequenceNumber and eventName cannot be empty");
    }
  }

  @Override
  public String toString() {
    return "Event{"
        + this.entityReference()
        + ":" + this.sequenceNumber()
        + "(" + this.eventName() + ")}";
  }
}
