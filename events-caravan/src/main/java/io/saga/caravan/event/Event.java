package io.saga.caravan.event;

import io.saga.caravan.entity.EntityReference;
import lombok.Builder;
import org.jspecify.annotations.Nullable;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;

import static io.saga.caravan.utils.TextUtils.hasText;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;

@Builder
public record Event<T>(EntityReference entityReference,
                       String eventName,
                       long sequenceNumber,
                       ZonedDateTime timestamp,
                       T payload) {

  private static final ZoneId Z = ZoneId.of("Z");

  public Event {
    requireNonNull(entityReference, "entityReference cannot be null");

    if (sequenceNumber <= 0) {
      throw new IllegalArgumentException("sequenceNumber must be positive number");
    }

    if (!hasText(eventName)) {
      throw new IllegalArgumentException("eventName must contain text");
    }

    requireNonNull(timestamp, "timestamp cannot be null");
    timestamp = normalize(timestamp);

    requireNonNull(payload, "payload cannot be null");
  }

  private ZonedDateTime normalize(ZonedDateTime zonedDateTime) {
    return zonedDateTime.truncatedTo(MILLIS).withZoneSameInstant(Z);
  }

  public EventReference eventReference() {
    return new EventReference(
        this.entityReference(),
        this.sequenceNumber(),
        this.eventName());
  }

  public EventType eventType() {
    return new EventType(
        this.entityReference().entityName(),
        this.eventName());
  }

  @Override
  public String toString() {
    return this.eventReference().toString();
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    Event<?> event = (Event<?>) o;
    return Objects.equals(this.entityReference, event.entityReference)
        && this.sequenceNumber == event.sequenceNumber;
  }

  @Override
  public int hashCode() {
    return Objects.hash(entityReference, sequenceNumber);
  }
}
