package dev.baitursinov.caravan.event.producer;

/**
 * Writes events as members of their entities' sequences, arbitrating those sequences. Implemented
 * by extenders for a specific technology; applications do not call it directly, but produce events
 * through a {@link SequencedEventProducer}, which validates them before handing them to this
 * writer. Writers only accept {@link ValidatedEvents}, which only the producer can create, so
 * calling a writer directly cannot bypass that validation.
 *
 * <p>Implementations must ensure that:
 * <ul>
 *   <li>every event written without an exception is durable and eventually delivered to all
 *   consumers at least once;</li>
 *   <li>a batch is written atomically: all of its events or none;</li>
 *   <li>writing an event whose entity already has an event with the same {@code sequenceNumber}
 *   fails synchronously with a {@link DuplicateEventProductionException}, signaling either a retry
 *   of an already written event or a concurrent modification of that entity. Of several concurrent
 *   writers of the same entity and sequence number exactly one succeeds.</li>
 * </ul>
 */
public interface SequencedEventWriter {

  /**
   * Writes a batch of events. An empty batch writes nothing.
   *
   * @throws DuplicateEventProductionException if any of the events already exists, in which case
   *                                           none is written
   * @throws EventProductionException          if the events cannot be written, in which case none
   *                                           is
   */
  void write(ValidatedEvents events);
}
