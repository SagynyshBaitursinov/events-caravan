package dev.baitursinov.caravan.event.sourcing.entity.stream;

import static java.util.Objects.requireNonNull;

/**
 * Declares that entities of the given {@code entityName} should be recorded into the
 * entity stream, and how their stream location is derived.
 * <p>
 * Entities whose entityName has no registration are not written into the entity stream by
 * {@link EntityStreamWritingEventHandler}.
 * <p>
 * Applications supply a collection of these to
 * {@link EntityStreamRegistry#createFor(java.util.Collection)} to build the registry consulted
 * by {@link EntityStreamWritingEventHandler}.
 *
 * @param entityName the entity type to be recorded into the entity stream
 * @param timeBucket granularity entities of this type are bucketed by creation time;
 *                   must stay fixed once the stream is populated
 * @param shardCount number of shards each time bucket of this entity type is split
 *                   into; must stay fixed once the stream is populated
 */
public record EntityStreamRegistration(String entityName,
                                       TimeBucket timeBucket,
                                       int shardCount) {

  public EntityStreamRegistration {
    requireNonNull(entityName, "entityName cannot be null");
    requireNonNull(timeBucket, "timeBucket cannot be null");

    if (shardCount <= 0) {
      throw new EntityStreamRegistrationException(
          "shardCount must be positive, got %d".formatted(shardCount));
    }
  }
}
