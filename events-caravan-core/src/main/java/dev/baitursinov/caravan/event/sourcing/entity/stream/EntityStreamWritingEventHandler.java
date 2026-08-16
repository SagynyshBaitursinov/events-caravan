package dev.baitursinov.caravan.event.sourcing.entity.stream;

import dev.baitursinov.caravan.entity.EntityReference;
import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.consumer.handler.EventHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Write every entity into the stream upon its first event, provided its entityName has an
 * {@link EntityStreamRegistration} in the {@link EntityStreamRegistry}. Unregistered entities are
 * not written into the stream.
 */
@Slf4j
@RequiredArgsConstructor
public final class EntityStreamWritingEventHandler implements EventHandler<Object> {

  private static final long FIRST_SEQUENCE_NUMBER = 1;

  private final EntityStreamWriter entityStreamWriter;
  private final EntityStreamRegistry entityStreamRegistry;

  @Override
  public boolean isOfInterest(Event<Object> event) {
    return event.sequenceNumber() == FIRST_SEQUENCE_NUMBER
        && entityStreamRegistry.registrationFor(event.entityReference().entityName()).isPresent();
  }

  @Override
  public void handle(Event<Object> event) {
    var entityReference = event.entityReference();
    var registration = getRegistration(entityReference);

    var timeBucketLocation = registration.timeBucket().locationOf(event.timestamp());
    var shardLocation = getShardLocation(entityReference, registration.shardCount());

    entityStreamWriter.write(
        new EntityStreamEntry(entityReference, event.timestamp()),
        timeBucketLocation,
        shardLocation);

    log.debug("Wrote {} to stream at timeBucket={}, shard={}",
        entityReference, timeBucketLocation, shardLocation);
  }

  private EntityStreamRegistration getRegistration(EntityReference entityReference) {
    return entityStreamRegistry.registrationFor(entityReference.entityName())
        .orElseThrow(() -> new IllegalStateException(
            "No EntityStreamRegistration for entityName=%s"
                .formatted(entityReference.entityName())));
  }

  private int getShardLocation(EntityReference entityReference, int shardCount) {
    return Math.floorMod(Fnv1a64.hash(entityReference.entityId()), shardCount);
  }
}
