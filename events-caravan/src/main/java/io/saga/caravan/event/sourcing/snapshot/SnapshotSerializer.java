package io.saga.caravan.event.sourcing.snapshot;

/**
 * Serializes a snapshot's payload to a wire format such as JSON, for storage by a
 * {@link SnapshotStore}. A default Jackson-based implementation is provided; implement this to
 * use a different format.
 */
public interface SnapshotSerializer {

  /**
   * @throws SnapshotException if the payload cannot be serialized
   */
  String serializePayload(EntitySnapshot<?> entitySnapshot);
}
