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

public abstract class EventSourcedRepository<T extends EventSourcedEntity> implements Repository<T> {

  private final String entityName;
  private final Class<T> entityClass;

  private final EventStore eventStore;
  private final EventProducer eventProducer;
  private final SnapshotStore snapshotStore;

  @Nullable
  private final SnapshotTaker<T, ?> snapshotTaker;

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

  protected abstract T createWithBlankState(String entityId);

  public final String entityName() {
    return entityName;
  }

  public final Class<T> entityClass() {
    return entityClass;
  }
}
