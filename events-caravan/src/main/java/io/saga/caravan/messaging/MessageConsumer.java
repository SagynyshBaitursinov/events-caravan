package io.saga.caravan.messaging;

public interface MessageConsumer {

  void consume(Message message);
}
