package io.saga.caravan.event.sourcing.snapshot;

import io.saga.caravan.entity.EntityReference;

import java.util.Optional;

/**
 * Persists and retrieves {@link EntitySnapshot}s. Implemented for a specific
 * storage backend;
 */
public interface SnapshotStore {

  /**
   * Persists the given snapshot, replacing any snapshot previously stored for the same entity.
   *
   * @throws SnapshotException if the snapshot cannot be saved
   */
  void save(EntitySnapshot<?> snapshot);

  /**
   * Loads the latest snapshot stored for the given entity, if any.
   *
   * @throws SnapshotException if a stored snapshot exists but cannot be read
   */
  <S> Optional<EntitySnapshot<S>> load(EntityReference entityReference,
                                       Class<S> snapshotClass);
}
