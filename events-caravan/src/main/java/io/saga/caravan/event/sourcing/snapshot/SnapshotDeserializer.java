package io.saga.caravan.event.sourcing.snapshot;

public interface SnapshotDeserializer {

  <S> S deserializePayload(String snapshotPayloadAsJson,
                           Class<S> snapshotPayloadClass);
}
