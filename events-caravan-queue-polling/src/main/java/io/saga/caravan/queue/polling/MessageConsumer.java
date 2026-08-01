package io.saga.caravan.queue.polling;

public interface MessageConsumer {

  void consume(Message message);
}
