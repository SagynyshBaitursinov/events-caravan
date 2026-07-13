package io.saga.caravan.event.consumer.messaging;

import io.saga.caravan.event.Event;
import io.saga.caravan.event.consumer.EventConsumer;
import io.saga.caravan.event.serialization.EventDeserializationException;
import io.saga.caravan.event.serialization.EventDeserializer;
import io.saga.caravan.messaging.Message;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class EventMessageConsumer {

  private final EventDeserializer eventDeserializer;
  private final EventConsumer eventConsumer;

  public void consume(Message eventMessage) {
    Event<?> event = deserialize(eventMessage);

    try {
      eventConsumer.consume(event);
    } catch (Exception exception) {
      throw new EventMessageConsumptionException(event, exception);
    }
  }

  private Event<?> deserialize(Message eventMessage) {
    try {
      return eventDeserializer.deserialize(eventMessage.body());
    } catch (EventDeserializationException exception) {
      throw new EventMessageConsumptionException(exception);
    }
  }
}
