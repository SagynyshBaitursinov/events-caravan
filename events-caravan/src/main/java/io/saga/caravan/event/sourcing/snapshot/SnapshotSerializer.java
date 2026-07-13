package io.saga.caravan.event.sourcing.snapshot;

public interface SnapshotSerializer {

  String serializePayload(EntitySnapshot<?> entitySnapshot);
}
