package io.saga.caravan.event.sourcing.snapshot;

import io.saga.caravan.entity.EntityReference;
import io.saga.caravan.event.sourcing.EventSourcedEntity;
import lombok.RequiredArgsConstructor;

/**
 * Defines how snapshots are taken and restored for one entity type {@code E}, using a snapshot
 * payload type {@code S}. Applications implement this and register it with an
 * {@code EventSourcingRepositoryContext} to enable snapshotting for that entity type; without
 * one, its repository always restores entities by replaying every event.
 *
 * @param <E> the entity type this snapshot taker handles
 * @param <S> the type of the captured snapshot payload
 */
@RequiredArgsConstructor
public abstract class SnapshotTaker<E extends EventSourcedEntity, S> {

  private final Class<E> entityClass;
  private final Class<S> snapshotClass;

  /**
   * Captures the given entity's current state as a snapshot payload.
   */
  public abstract S takeSnapshot(E entity);

  /**
   * Rebuilds an entity instance from a previously captured snapshot payload, restoring the
   * entity's state as of the snapshot's version.
   */
  public abstract E recreateFromSnapshot(EntityReference entityReference,
                                         S snapshotPayload);

  /**
   * How often, in number of events, a new snapshot is taken: after saving an entity, a snapshot
   * is taken if any newly recorded event's sequence number is a multiple of this value.
   * <p>
   * Must be a positive number.
   */
  public abstract int frequencyOfSnapshots();

  /**
   * The event {@link EventSourcedEntity} class this snapshot taker handles.
   */
  public final Class<E> entityClass() {
    return entityClass;
  }

  /**
   * The class of the snapshot payload.
   */
  public final Class<S> snapshotClass() {
    return snapshotClass;
  }
}
