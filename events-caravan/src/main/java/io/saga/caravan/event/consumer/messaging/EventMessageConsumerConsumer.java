package io.saga.caravan.event.consumer.messaging;

import io.saga.caravan.event.Event;
import io.saga.caravan.event.consumer.EventConsumer;
import io.saga.caravan.event.serialization.EventDeserializationException;
import io.saga.caravan.event.serialization.EventDeserializer;
import io.saga.caravan.messaging.Message;
import io.saga.caravan.messaging.MessageConsumer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class EventMessageConsumerConsumer implements MessageConsumer {

  private final EventDeserializer eventDeserializer;
  private final EventConsumer eventConsumer;

  @Override
  public void accept(Message eventMessage) {
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
