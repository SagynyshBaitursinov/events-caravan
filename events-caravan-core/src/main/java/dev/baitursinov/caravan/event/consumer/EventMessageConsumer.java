package dev.baitursinov.caravan.event.consumer;

import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.serialization.EventDeserializationException;
import dev.baitursinov.caravan.event.serialization.EventDeserializer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Bridges a raw, serialized event message to an {@link EventConsumer}: deserializes it with the
 * given {@link EventDeserializer}, then hands the resulting {@link Event} to the consumer.
 */
@Slf4j
@RequiredArgsConstructor
public class EventMessageConsumer {

  private final EventDeserializer eventDeserializer;
  private final EventConsumer eventConsumer;

  /**
   * Deserializes {@code messageBody} and passes it to the underlying {@link EventConsumer}.
   *
   * @throws EventMessageConsumptionException if deserialization fails or the underlying
   *                                          consumer throws
   */
  public void consume(String messageBody) {
    log.debug("Consuming a message");

    Event<?> event = deserialize(messageBody);

    try {
      eventConsumer.consume(event);
    } catch (Exception exception) {
      throw new EventMessageConsumptionException(event, exception);
    }
  }

  private Event<?> deserialize(String messageBody) {
    try {
      return eventDeserializer.deserialize(messageBody);
    } catch (EventDeserializationException exception) {
      throw new EventMessageConsumptionException(exception);
    }
  }
}
