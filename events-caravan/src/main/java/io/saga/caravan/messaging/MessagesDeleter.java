package io.saga.caravan.messaging;

import java.util.List;

public interface MessagesDeleter {

  void delete(List<Message> messages);
}
