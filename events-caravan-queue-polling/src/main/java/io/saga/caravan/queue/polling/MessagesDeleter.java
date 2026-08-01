package io.saga.caravan.queue.polling;

import java.util.List;

public interface MessagesDeleter {

  void delete(List<Message> messages);
}
