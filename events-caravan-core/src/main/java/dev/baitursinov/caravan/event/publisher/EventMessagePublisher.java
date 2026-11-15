package dev.baitursinov.caravan.event.publisher;

import java.util.List;

/**
 * Transport-specific side of publishing: puts serialized {@link EventMessage}s into a message
 * broker. Implemented by extenders for a concrete broker, the sending counterpart of a
 * queue-polling transport feeding {@link dev.baitursinov.caravan.event.consumer.EventMessageConsumer}.
 *
 * <p>Implementations must deliver at-least-once: either every given message is accepted by the
 * broker, or an {@link EventPublicationException} naming the failed messages is thrown.
 */
public interface EventMessagePublisher {

  /**
   * @throws EventPublicationException if the message was not accepted by the broker
   */
  void publish(EventMessage eventMessage);

  /**
   * Publishes a batch of messages, in any order.
   *
   * @throws EventPublicationException if any of the messages was not accepted by the broker; the
   *                                   exception names the failed ones, others may have been published
   */
  void publish(List<EventMessage> eventMessages);
}
