package dev.baitursinov.caravan.event.sourcing.entity.stream;

/**
 * Writes entities into an entity stream, so that all entities can be iterated over.
 * Can be implemented for specific storage backends.
 *
 * <p>Implementations must be idempotent: registering the same {@link EntityStreamEntry} more than
 * once (e.g. due to at-least-once redelivery of its first event) must be a no-op, not an error.
 */
public interface EntityStreamWriter {

  /**
   * Writes the given entity into the stream, or does nothing if it is already written.
   */
  void write(EntityStreamEntry entry);
}
