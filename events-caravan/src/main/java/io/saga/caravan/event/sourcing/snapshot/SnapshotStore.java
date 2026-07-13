package io.saga.caravan.event.sourcing.snapshot;

import io.saga.caravan.entity.EntityReference;

import java.util.Optional;

public interface SnapshotStore {

  void save(EntitySnapshot<?> snapshot);

  <S> Optional<EntitySnapshot<S>> load(EntityReference entityReference,
                                       Class<S> snapshotClass);
}
