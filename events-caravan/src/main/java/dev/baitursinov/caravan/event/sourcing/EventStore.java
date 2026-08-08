package dev.baitursinov.caravan.event.sourcing;

import dev.baitursinov.caravan.entity.EntityReference;
import dev.baitursinov.caravan.event.Event;

import java.util.stream.Stream;

/**
 * Reads back events previously produced by an {@link dev.baitursinov.caravan.event.producer.EventProducer}.
 * Implemented by extenders for a specific storage backend; used by
 * {@link EventSourcedRepository} to restore an entity's state.
 */
public interface EventStore {

  /**
   * Streams an entity's events, in ascending sequence-number order, starting after
   * {@code fromSequenceNumberExclusive}.
   *
   * @param entityReference             identifies the entity whose events to read
   * @param fromSequenceNumberExclusive the sequence number to start after; 0 to read from the
   *                                    beginning
   * @throws EventStoreException if the events cannot be read
   */
  Stream<Event<?>> getEventsOfEntity(EntityReference entityReference,
                                     long fromSequenceNumberExclusive);
}
