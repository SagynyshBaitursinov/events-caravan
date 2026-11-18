package dev.baitursinov.caravan.event.producer;

import dev.baitursinov.caravan.event.EntityEventsRegistry;
import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.SequencedEventReference;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Produces sequenced events, every one as the next member of its entity's sequence, identified by
 * a {@link SequencedEventReference}. Used by applications and
 * {@link dev.baitursinov.caravan.event.sourcing.EventSourcedRepository}; validates each event
 * against the {@link EntityEventsRegistry} and hands it to a technology-specific
 * {@link SequencedEventWriter}.
 *
 * <p>Events accepted without an exception are durable and eventually delivered to all consumers
 * at least once. A batch is produced atomically, and producing an event whose entity already has
 * an event with the same {@code sequenceNumber} fails with a
 * {@link DuplicateEventProductionException}.
 */
public final class SequencedEventProducer {

  private final SequencedEventWriter writer;
  private final EventPayloadClassValidator eventPayloadClassValidator;

  public SequencedEventProducer(SequencedEventWriter writer, EntityEventsRegistry entityEventsRegistry) {
    this.writer = requireNonNull(writer);
    this.eventPayloadClassValidator = new EventPayloadClassValidator(requireNonNull(entityEventsRegistry));
  }

  /**
   * Produces a single event.
   *
   * @throws DuplicateEventProductionException if the event already exists
   * @throws EventProductionException          if the event is not sequenced or not valid, or
   *                                           cannot be produced
   */
  public void produce(Event<?> event) {
    produce(List.of(event));
  }

  /**
   * Produces a batch of events: all of them or none.
   *
   * @throws DuplicateEventProductionException if any of the events already exists
   * @throws EventProductionException          if any of the events is not sequenced or not valid,
   *                                           or cannot be produced
   */
  public void produce(List<Event<?>> events) {
    events.forEach(SequencedEventProducer::requireSequenced);
    events.forEach(eventPayloadClassValidator::validate);
    writer.write(new ValidatedEvents(events));
  }

  private static void requireSequenced(Event<?> event) {
    if (!(event.eventReference() instanceof SequencedEventReference)) {
      throw new EventProductionException(
          "Cannot produce %s, because it has no sequenceNumber; produce it with %s instead"
              .formatted(event.eventReference(), UnsequencedEventProducer.class.getSimpleName()));
    }
  }
}
