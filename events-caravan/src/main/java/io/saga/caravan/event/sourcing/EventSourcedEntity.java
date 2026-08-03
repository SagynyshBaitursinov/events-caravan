package io.saga.caravan.event.sourcing;

import io.saga.caravan.entity.Entity;
import io.saga.caravan.event.Event;
import lombok.extern.slf4j.Slf4j;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Base class for an entity whose state is derived from a sequence of {@link Event}s.
 * Applications extending this class must annotate it with {@link EntityName}, and
 * mutate state exclusively by calling {@link #recordEvent(String, Object)} from
 * domain-logic methods;
 * For applying the resulting state changes immediately after event is recorded
 * or when entity state is loaded later, methods annotated with
 * {@link io.saga.caravan.event.sourcing.applying.ApplyEvent} are utilized.
 *
 * <p>Instances are saved and loaded by an {@link EventSourcedRepository} implementations,
 * which load an entity state by replaying its events (or a snapshot plus the events since).
 * Saving entity state happen by producing the events recorded since it was loaded.
 */
@Slf4j
public abstract class EventSourcedEntity extends Entity {

  private static final ClassValue<String> ENTITY_NAMES = new ClassValue<>() {
    @Override
    protected String computeValue(Class<?> entityClass) {
      var eventName = entityClass.getDeclaredAnnotation(EntityName.class);

      if (eventName == null) {
        throw new EventSourcedEntitySetupException(
            "%s must declare its entityName with @EntityName"
                .formatted(entityClass.getName()));
      }

      return eventName.value();
    }
  };

  private final List<Event<?>> notProducedEvents = new ArrayList<>();

  private long version = 0L;

  static String entityNameOf(Class<? extends EventSourcedEntity> entityClass) {
    return ENTITY_NAMES.get(entityClass);
  }

  @Override
  public final String entityName() {
    return entityNameOf(this.getClass());
  }

  /**
   * The sequence number of the last event applied to this entity; 0 for a blank state
   * entity, which cannot be yet saved or loaded.
   */
  public final long version() {
    return version;
  }

  final void setVersion(long version) {
    this.version = version;
  }

  /**
   * Records a new event with the given name and payload against this entity, immediately
   * applying it via the matching {@link io.saga.caravan.event.sourcing.applying.ApplyEvent}
   * method so this entity's in-memory state reflects it. The event is queued to be produced the
   * next time this entity is saved through its {@link EventSourcedRepository}.
   *
   * @param eventName    the name of the event, as declared in the application's event registry
   * @param eventPayload the payload describing the details of what happened
   */
  protected final <E> void recordEvent(String eventName,
                                       E eventPayload) {
    var event = buildEvent(eventName, eventPayload);
    log.debug("Recording {}", event.eventReference());

    EntityEventApplier.apply(this, event);

    notProducedEvents.add(event);
  }

  private <E> Event<E> buildEvent(String eventName,
                                  E eventPayload) {
    return Event.<E>builder()
        .entityReference(this.entityReference())
        .sequenceNumber(this.version + 1)
        .eventName(eventName)
        .timestamp(ZonedDateTime.now())
        .payload(eventPayload)
        .build();
  }

  final List<Event<?>> notProducedEvents() {
    return Collections.unmodifiableList(notProducedEvents);
  }

  final void clearNotProducedEvents() {
    notProducedEvents.clear();
  }

  final boolean hasBlankState() {
    return version == 0L;
  }
}