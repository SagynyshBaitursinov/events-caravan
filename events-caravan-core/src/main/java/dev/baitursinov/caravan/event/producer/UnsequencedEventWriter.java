package dev.baitursinov.caravan.event.producer;

/**
 * Writes events as facts that are independent of one another, rather than as members of their
 * entities' sequences. Implemented by extenders for a specific technology.
 *
 * <p>Implementations must ensure that every event written without an exception is eventually
 * delivered to all consumers at least once. No more guarantees are required: a batch need not be atomic,
 * duplicates need not be detected and no delivery order is promised. Implementations may guarantee
 * more, (e.g. an outbox writing a batch in one transaction), and should document it if they do.
 */
public interface UnsequencedEventWriter {

  /**
   * Writes a batch of events. An empty batch writes nothing.
   *
   * @throws EventProductionException if the events cannot be written
   */
  void write(ValidatedEvents events);
}
