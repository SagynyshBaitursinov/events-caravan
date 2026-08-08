package dev.baitursinov.caravan.event.sourcing.snapshot;

/**
 * Deserializes a snapshot's payload from the wire format produced by a matching
 * {@link SnapshotSerializer}, as read back from a {@link SnapshotStore}.
 */
public interface SnapshotDeserializer {

  /**
   * @throws SnapshotException if the payload cannot be deserialized
   */
  <S> S deserializePayload(String snapshotPayloadAsJson,
                           Class<S> snapshotPayloadClass);
}
