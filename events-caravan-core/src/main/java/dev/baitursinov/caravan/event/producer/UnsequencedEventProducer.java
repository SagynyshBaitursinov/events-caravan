package dev.baitursinov.caravan.event.producer;

import dev.baitursinov.caravan.event.EntityEventsRegistry;
import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.UnsequencedEventReference;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Produces events as facts that are independent of one another, rather than as members of their
 * entities' sequences. It is meant for events where nobody relies on an event being the N-th
 * thing that happened to its entity. Such events have no {@code sequenceNumber}: they are
 * identified by an {@link UnsequencedEventReference}, carrying a {@code naturalKey} given by the
 * caller, which has to stay the same when producing the same event again on retry, so that
 * consumers can recognize duplicates; neither this producer nor the framework skips them.
 * Validates each event against the {@link EntityEventsRegistry} and hands it
 * to a technology-specific {@link UnsequencedEventWriter}.
 *
 * <p>Events accepted without an exception are eventually delivered to all consumers at least
 * once. Unlike {@link SequencedEventProducer} it guarantees nothing more: a batch is not guaranteed
 * to be atomic, duplicates are not detected on production and no delivery order is promised.
 * Stronger guarantees can only come from the {@link UnsequencedEventWriter} implementation in use.
 */
public final class UnsequencedEventProducer {

  private final UnsequencedEventWriter writer;
  private final EventPayloadClassValidator eventPayloadClassValidator;

  public UnsequencedEventProducer(UnsequencedEventWriter writer, EntityEventsRegistry entityEventsRegistry) {
    this.writer = requireNonNull(writer);
    this.eventPayloadClassValidator = new EventPayloadClassValidator(requireNonNull(entityEventsRegistry));
  }

  /**
   * Produces a single event.
   *
   * @throws EventProductionException if the event is sequenced or not valid, or cannot be
   *                                  produced
   */
  public void produce(Event<?> event) {
    produce(List.of(event));
  }

  /**
   * Produces a batch of events. Invalid events fail the whole batch before any is produced.
   *
   * @throws EventProductionException if any of the events is sequenced or not valid, or cannot be
   *                                  produced
   */
  public void produce(List<Event<?>> events) {
    events.forEach(UnsequencedEventProducer::requireUnsequenced);
    events.forEach(eventPayloadClassValidator::validate);
    writer.write(new ValidatedEvents(events));
  }

  private static void requireUnsequenced(Event<?> event) {
    if (!(event.eventReference() instanceof UnsequencedEventReference)) {
      throw new EventProductionException(
          "Cannot produce %s, because it has a sequenceNumber; produce it with %s instead"
              .formatted(event.eventReference(), SequencedEventProducer.class.getSimpleName()));
    }
  }
}
