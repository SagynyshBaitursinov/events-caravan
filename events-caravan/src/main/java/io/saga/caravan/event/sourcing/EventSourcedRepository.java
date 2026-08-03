package io.saga.caravan.event.sourcing;

import io.saga.caravan.entity.EntityReference;
import io.saga.caravan.entity.Repository;
import io.saga.caravan.event.Event;
import io.saga.caravan.event.producer.DuplicateEventProductionException;
import io.saga.caravan.event.producer.EventProducer;
import io.saga.caravan.event.sourcing.snapshot.EntitySnapshot;
import io.saga.caravan.event.sourcing.snapshot.SnapshotStore;
import io.saga.caravan.event.sourcing.snapshot.SnapshotTaker;
import org.jspecify.annotations.Nullable;

import java.util.Optional;

/**
 * A {@link Repository} for an event-sourced entity of type {@code T}. Applications extend this
 * class per entity type, having to implement only {@link #createWithBlankState(String)}.
 *
 * <p>{@link #findBy(String)} restores an entity by loading its latest snapshot
 * (if a {@link SnapshotTaker} is configured for it and snapshot already exists)
 * and replaying the events recorded since.
 * <p>
 * Saving via {@link #save(T)} produces the events recorded on the entity since it was loaded and,
 * depending on the configured {@link SnapshotTaker}'s frequency, takes a new snapshot.
 * There's no guarantee that taking snapshot succeeds after events are produced atomically.
 *
 * @param <T> the concrete {@link EventSourcedEntity} type this repository manages
 */
public abstract class EventSourcedRepository<T extends EventSourcedEntity> implements Repository<T> {

  private final String entityName;
  private final Class<T> entityClass;

  private final EventStore eventStore;
  private final EventProducer eventProducer;
  private final SnapshotStore snapshotStore;

  @Nullable
  private final SnapshotTaker<T, ?> snapshotTaker;

  /**
   * @param entityClass the concrete entity class this repository manages
   * @param context     the shared context this repository draws its {@link EventStore},
   *                    {@link EventProducer} and {@link SnapshotStore} from, and registers
   *                    itself with
   * @throws EventSourcedEntitySetupException if another repository is already registered for
   *                                          the same entity name, or the entity class's
   *                                          {@code @ApplyEvent} methods don't match its
   *                                          registered events
   */
  protected EventSourcedRepository(Class<T> entityClass,
                                   EventSourcingRepositoryContext context) {
    this.entityName = EventSourcedEntity.entityNameOf(entityClass);
    this.entityClass = entityClass;

    this.eventStore = context.eventStore();
    this.eventProducer = context.eventProducer();
    this.snapshotStore = context.snapshotStore();
    this.snapshotTaker = context.snapshotTakerFor(entityClass);

    context.register(this);
  }

  /**
   * @throws EventSourcedRepositoryException if the entity has blank-state or
   *                                         is not of the exact class this repository
   *                                         manages, or its events cannot be produced (e.g. it
   *                                         was concurrently modified elsewhere)
   */
  @Override
  public final void save(T entity) {
    validateEntityClass(entity);

    if (entity.hasBlankState()) {
      throw new EventSourcedRepositoryException(
          "Cannot save a blank %s with no events recorded"
              .formatted(entity.entityReference()));
    }

    produceEvents(entity);

    if (shouldTakeSnapshot(entity)) {
      takeSnapshot(entity);
    }

    entity.clearNotProducedEvents();
  }

  private void produceEvents(T entity) {
    try {
      eventProducer.produce(entity.notProducedEvents());
    } catch (DuplicateEventProductionException exception) {
      throw new EventSourcedRepositoryException(
          "Cannot modify one entity in parallel",
          exception);
    } catch (RuntimeException exception) {
      throw new EventSourcedRepositoryException(
          "Cannot produce entity events",
          exception);
    }
  }

  private void takeSnapshot(T entity) {
    if (snapshotTaker() == null) {
      return;
    }

    snapshotStore.save(
        EntitySnapshot.builder()
            .entityReference(entity.entityReference())
            .version(entity.version())
            .payload(snapshotTaker().takeSnapshot(entity))
            .build());
  }

  private boolean shouldTakeSnapshot(T entity) {
    if (snapshotTaker() == null) {
      return false;
    }

    return entity.notProducedEvents().stream()
        .map(Event::sequenceNumber)
        .anyMatch(this::matchesSnapshotFrequency);
  }

  private boolean matchesSnapshotFrequency(Long eventVersion) {
    if (snapshotTaker() == null) {
      return false;
    }

    return eventVersion % snapshotTaker().frequencyOfSnapshots() == 0;
  }

  /**
   * @throws EventSourcedRepositoryException if the restored entity is not of the exact class
   *                                         this repository manages
   */
  @Override
  public final Optional<T> findBy(String entityId) {
    return findBy(createEntityReference(entityId));
  }

  private Optional<T> findBy(EntityReference entityReference) {
    T entity = getSnapshotState(entityReference)
        .orElseGet(() -> createWithBlankState(entityReference.entityId()));

    validateEntity(entity, entityReference);

    eventStore.getEventsOfEntity(entityReference, entity.version())
        .forEach(event -> EntityEventApplier.apply(entity, event));

    if (entity.hasBlankState()) {
      return Optional.empty();
    } else {
      return Optional.of(entity);
    }
  }

  private Optional<T> getSnapshotState(EntityReference entityReference) {
    var snapshotTaker = snapshotTaker();
    if (snapshotTaker == null) {
      return Optional.empty();
    }

    return recreateFromSnapshot(snapshotStore, snapshotTaker, entityReference);
  }

  private void validateEntity(T entity,
                              EntityReference entityReference) {
    validateEntityClass(entity);

    if (!entity.entityReference().equals(entityReference)) {
      throw new EventSourcedRepositoryException(
          "%s asked for %s but got %s"
              .formatted(
                  this.getClass().getName(),
                  entityReference,
                  entity.entityReference()));
    }
  }

  private void validateEntityClass(T entity) {
    if (entity.getClass() != entityClass) {
      throw new EventSourcedRepositoryException(
          "%s handles entityClass=%s, but got an instance of %s; an event sourced entity must be of exactly the class its repository declares"
              .formatted(
                  this.getClass().getName(),
                  entityClass.getName(),
                  entity.getClass().getName()));
    }
  }

  private <E extends EventSourcedEntity, S> Optional<E> recreateFromSnapshot(
      SnapshotStore snapshotStore,
      SnapshotTaker<E, S> snapshotTaker,
      EntityReference entityReference) {

    return snapshotStore
        .load(
            entityReference,
            snapshotTaker.snapshotClass())
        .map(snapshot -> {
          var recreatedEntity = snapshotTaker.recreateFromSnapshot(entityReference, snapshot.payload());
          recreatedEntity.setVersion(snapshot.version());
          return recreatedEntity;
        });
  }

  private EntityReference createEntityReference(String entityId) {
    return new EntityReference(entityName(), entityId);
  }

  @Nullable
  private SnapshotTaker<T, ?> snapshotTaker() {
    return snapshotTaker;
  }

  /**
   * Creates a new instance of the entity with no state yet applied, with the given id, ready to
   * have events applied to it during {@link #findBy(String)} or ready to record its first event.
   */
  protected abstract T createWithBlankState(String entityId);

  /**
   * The entity name this repository manages, as declared via {@link EntityName} on
   * {@code entityClass}.
   */
  public final String entityName() {
    return entityName;
  }

  /**
   * The concrete entity class this repository manages.
   */
  public final Class<T> entityClass() {
    return entityClass;
  }
}
