package dev.baitursinov.caravan.event;

import dev.baitursinov.caravan.entity.EntityReference;
import lombok.NoArgsConstructor;
import org.jspecify.annotations.Nullable;

import java.time.ZonedDateTime;

import static lombok.AccessLevel.PACKAGE;

/**
 * Builds a sequenced {@link Event}, the {@code sequenceNumber}-th event of its entity, identified
 * by a {@link SequencedEventReference}. Obtained from {@link Event#sequenced()}.
 *
 * @param <T> the type of the event's {@code payload}
 */
@NoArgsConstructor(access = PACKAGE)
public final class SequencedEventBuilder<T> {

  private @Nullable EntityReference entityReference;
  private @Nullable String eventName;
  private long sequenceNumber;
  private @Nullable ZonedDateTime timestamp;
  private @Nullable T payload;

  public SequencedEventBuilder<T> entityReference(EntityReference entityReference) {
    this.entityReference = entityReference;
    return this;
  }

  public SequencedEventBuilder<T> eventName(String eventName) {
    this.eventName = eventName;
    return this;
  }

  public SequencedEventBuilder<T> sequenceNumber(long sequenceNumber) {
    this.sequenceNumber = sequenceNumber;
    return this;
  }

  public SequencedEventBuilder<T> timestamp(ZonedDateTime timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  public SequencedEventBuilder<T> payload(T payload) {
    this.payload = payload;
    return this;
  }

  @SuppressWarnings({"DataFlowIssue", "NullableProblems"})
  public Event<T> build() {
    return new Event<>(
        new SequencedEventReference(entityReference, sequenceNumber, eventName),
        timestamp,
        payload);
  }
}
