package io.saga.caravan.event.sourcing;

import io.saga.caravan.entity.EntityReference;
import io.saga.caravan.entity.Repository;
import io.saga.caravan.event.Event;
import io.saga.caravan.event.producer.DuplicateEventProductionException;
import io.saga.caravan.event.producer.EventProducer;
import io.saga.caravan.event.sourcing.snapshot.EntitySnapshot;
import io.saga.caravan.event.sourcing.snapshot.SnapshotStore;
import io.saga.caravan.event.sourcing.snapshot.SnapshotTaker;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.jspecify.annotations.Nullable;

import java.util.Optional;

@RequiredArgsConstructor
@Setter(AccessLevel.PACKAGE)
public abstract class EventSourcedEntityRepository<T extends EventSourcedEntity> implements Repository<T> {

  private final String entityName;
  private final Class<T> entityClass;

  private EventStore eventStore;
  private EventProducer eventProducer;
  private EntityEventApplier entityEventApplier;
  private SnapshotStore snapshotStore;

  @Nullable
  private SnapshotTaker<T, ?> snapshotTaker;

  @Override
  public final void save(T entity) {
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
    if (snapshotTaker() == null
        || snapshotTaker().entityClass() != entity.getClass()) {
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

    eventStore.getEventsOfEntity(entityReference, entity.version())
        .forEach(event -> entityEventApplier.apply(entity, event));

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
    return new EntityReference(entityName, entityId);
  }

  @Nullable
  private SnapshotTaker<T, ?> snapshotTaker() {
    return snapshotTaker;
  }

  @SuppressWarnings("unchecked")
  public void setSnapshotTaker(@Nullable SnapshotTaker<? extends EventSourcedEntity, ?> snapshotTaker) {
    if (snapshotTaker == null) {
      return;
    }

    if (this.entityClass != snapshotTaker.entityClass()) {
      throw new EventSourcedRepositoryException(
          "Cannot set snapshotTaker for entityClass=%s to repository of entityClass=%s"
              .formatted(snapshotTaker.entityClass(), this.entityClass));
    }

    this.snapshotTaker = (SnapshotTaker<T, ?>) snapshotTaker;
  }

  protected abstract T createWithBlankState(String entityId);

  public final String entityName() {
    return entityName;
  }

  public final Class<T> entityClass() {
    return entityClass;
  }
}
