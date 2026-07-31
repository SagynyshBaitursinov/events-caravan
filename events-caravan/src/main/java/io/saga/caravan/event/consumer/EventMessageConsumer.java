package io.saga.caravan.event.consumer;

import io.saga.caravan.event.Event;
import io.saga.caravan.event.serialization.EventDeserializationException;
import io.saga.caravan.event.serialization.EventDeserializer;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class EventMessageConsumer {

  private final EventDeserializer eventDeserializer;
  private final EventConsumer eventConsumer;

  public void consume(String eventMessage) {
    Event<?> event = deserialize(eventMessage);

    try {
      eventConsumer.consume(event);
    } catch (Exception exception) {
      throw new EventMessageConsumptionException(event, exception);
    }
  }

  private Event<?> deserialize(String eventMessage) {
    try {
      return eventDeserializer.deserialize(eventMessage);
    } catch (EventDeserializationException exception) {
      throw new EventMessageConsumptionException(exception);
    }
  }
}
