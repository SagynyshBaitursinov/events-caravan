package dev.baitursinov.caravan.event;

import dev.baitursinov.caravan.entity.EntityReference;
import lombok.NoArgsConstructor;
import org.jspecify.annotations.Nullable;

import java.time.ZonedDateTime;

import static lombok.AccessLevel.PACKAGE;

/**
 * Builds an unsequenced {@link Event}, independent of its entity's other events and identified by
 * an {@link UnsequencedEventReference}. Obtained from {@link Event#unsequenced()}.
 *
 * @param <T> the type of the event's {@code payload}
 */
@NoArgsConstructor(access = PACKAGE)
public final class UnsequencedEventBuilder<T> {

  private @Nullable EntityReference entityReference;
  private @Nullable String eventName;
  private @Nullable String naturalKey;
  private @Nullable ZonedDateTime timestamp;
  private @Nullable T payload;

  public UnsequencedEventBuilder<T> entityReference(EntityReference entityReference) {
    this.entityReference = entityReference;
    return this;
  }

  public UnsequencedEventBuilder<T> eventName(String eventName) {
    this.eventName = eventName;
    return this;
  }

  public UnsequencedEventBuilder<T> naturalKey(String naturalKey) {
    this.naturalKey = naturalKey;
    return this;
  }

  public UnsequencedEventBuilder<T> timestamp(ZonedDateTime timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  public UnsequencedEventBuilder<T> payload(T payload) {
    this.payload = payload;
    return this;
  }

  @SuppressWarnings({"DataFlowIssue", "NullableProblems"})
  public Event<T> build() {
    return new Event<>(
        new UnsequencedEventReference(entityReference, naturalKey, eventName),
        timestamp,
        payload);
  }
}
