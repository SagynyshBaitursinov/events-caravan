package dev.baitursinov.caravan.event.sourcing.entity.stream;

/**
 * Writes entities into an entity stream, so that all entities can be iterated over.
 * Can be implemented for specific storage backends.
 *
 * <p>Implementations must be idempotent: registering the same {@link EntityStreamEntry} at the
 * same location more than once (e.g. due to at-least-once redelivery of its first event) must be
 * a no-op, not an error.
 */
public interface EntityStreamWriter {

  /**
   * Writes the given entity into the stream at the given location, or does nothing if it is
   * already written there.
   *
   * @param entry              the entity to write into the stream
   * @param timeBucketLocation the time bucket {@code entry} is written into, as derived by
   *                           {@link EntityStreamWritingEventHandler} from the registered
   *                           {@link EntityStreamRegistration#timeBucket()} and Entity's first event's timestamp.
   * @param shardLocation      the shard within {@code timeBucketLocation} that {@code entry} is
   *                           written into, as derived by {@link EntityStreamWritingEventHandler}
   *                           from the registered {@link EntityStreamRegistration#shardCount()}
   *                           using Fnv1a64 hash on entityId.
   */
  void write(EntityStreamEntry entry, String timeBucketLocation, int shardLocation);
}
