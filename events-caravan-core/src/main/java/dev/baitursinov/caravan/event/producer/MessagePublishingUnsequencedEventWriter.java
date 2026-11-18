package dev.baitursinov.caravan.event.producer;

import dev.baitursinov.caravan.event.publisher.EventMessage;
import dev.baitursinov.caravan.event.publisher.EventMessagePublisher;
import dev.baitursinov.caravan.event.serialization.EventSerializer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * {@link UnsequencedEventWriter} guarantees that events end up in a message broker through an
 * {@link EventMessagePublisher}, synchronously with the caller.
 *
 * <p>The broker is the only destination, so an event that could not be published exists
 * nowhere: the caller gets an {@link EventProductionException} and has to retry.
 */
@Slf4j
@RequiredArgsConstructor
public class MessagePublishingUnsequencedEventWriter implements UnsequencedEventWriter {

  private final EventSerializer eventSerializer;
  private final EventMessagePublisher eventMessagePublisher;

  @Override
  public void write(ValidatedEvents events) {
    if (events.isEmpty()) {
      return;
    }

    try {
      eventMessagePublisher.publish(serialize(events));
    } catch (RuntimeException exception) {
      throw new EventProductionException(exception);
    }

    log.debug("Published {} event(s)", events.size());
  }

  private List<EventMessage> serialize(ValidatedEvents events) {
    return events.events().stream()
        .map(event -> new EventMessage(event.eventReference(), eventSerializer.serialize(event)))
        .toList();
  }
}
