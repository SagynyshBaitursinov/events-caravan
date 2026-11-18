package dev.baitursinov.caravan.event;

import dev.baitursinov.caravan.entity.EntityReference;
import org.jspecify.annotations.Nullable;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;

/**
 * A single fact that happened to an entity, identified by its {@link EventReference} and
 * carrying a {@code payload} describing details of what has happened.
 *
 * <p>An event is either sequenced, the {@code sequenceNumber}-th event of its entity
 * ({@link SequencedEventReference}), or unsequenced, independent of the entity's other events and
 * told apart from them by a {@code naturalKey} ({@link UnsequencedEventReference}).
 *
 * <p>{@code timestamp} is normalized to millisecond precision in UTC. Two events are
 * {@link #equals(Object) equal} when they share the same {@link EventReference}.
 *
 * @param <T> the type of {@code payload}
 */
public record Event<T>(EventReference eventReference,
                       ZonedDateTime timestamp,
                       T payload) {

  private static final ZoneId Z = ZoneId.of("Z");

  public Event {
    requireNonNull(eventReference, "eventReference cannot be null");

    requireNonNull(timestamp, "timestamp cannot be null");
    timestamp = normalize(timestamp);

    requireNonNull(payload, "payload cannot be null");
  }

  public static ZonedDateTime normalize(ZonedDateTime zonedDateTime) {
    return zonedDateTime.truncatedTo(MILLIS).withZoneSameInstant(Z);
  }

  /**
   * Starts building a sequenced event, the {@code sequenceNumber}-th event of its entity.
   */
  public static <T> SequencedEventBuilder<T> sequenced() {
    return new SequencedEventBuilder<>();
  }

  /**
   * Starts building an unsequenced event, independent of its entity's other events and told
   * apart from them by a {@code naturalKey}.
   */
  public static <T> UnsequencedEventBuilder<T> unsequenced() {
    return new UnsequencedEventBuilder<>();
  }

  public EntityReference entityReference() {
    return this.eventReference().entityReference();
  }

  public String eventName() {
    return this.eventReference().eventName();
  }

  /**
   * The kind of event this is: its entity's name paired with its {@code eventName}.
   */
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
    return Objects.equals(this.eventReference, event.eventReference);
  }

  @Override
  public int hashCode() {
    return Objects.hash(eventReference);
  }
}
