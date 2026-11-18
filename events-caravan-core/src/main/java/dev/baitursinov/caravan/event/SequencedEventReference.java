package dev.baitursinov.caravan.event;

import dev.baitursinov.caravan.entity.EntityReference;

import static dev.baitursinov.caravan.utils.TextUtils.hasText;
import static java.util.Objects.requireNonNull;

/**
 * Identifies the {@code sequenceNumber}-th event of its entity, named {@code eventName}.
 *
 * <p>Sequence numbers of an entity's events start at 1 and are gapless and increasing.
 */
public record SequencedEventReference(EntityReference entityReference,
                                      long sequenceNumber,
                                      String eventName) implements EventReference {

  public SequencedEventReference {
    requireNonNull(entityReference, "entityReference cannot be null");

    if (sequenceNumber <= 0) {
      throw new IllegalArgumentException("sequenceNumber must be positive number");
    }

    if (!hasText(eventName)) {
      throw new IllegalArgumentException("eventName cannot be empty");
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
