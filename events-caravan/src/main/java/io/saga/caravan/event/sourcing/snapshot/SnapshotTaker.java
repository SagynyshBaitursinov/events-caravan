package io.saga.caravan.event.sourcing.snapshot;

import io.saga.caravan.entity.EntityReference;
import io.saga.caravan.event.sourcing.EventSourcedEntity;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class SnapshotTaker<E extends EventSourcedEntity, S> {

  private final Class<E> entityClass;
  private final Class<S> snapshotClass;

  public abstract S takeSnapshot(E entity);

  public abstract E recreateFromSnapshot(EntityReference entityReference,
                                         S snapshotPayload);

  public abstract int frequencyOfSnapshots();

  public final Class<E> entityClass() {
    return entityClass;
  }

  public final Class<S> snapshotClass() {
    return snapshotClass;
  }
}
