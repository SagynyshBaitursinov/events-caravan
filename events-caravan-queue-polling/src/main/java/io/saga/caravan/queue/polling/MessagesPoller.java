package io.saga.caravan.queue.polling;

import java.util.Collection;

public interface MessagesPoller {

  Collection<Message> poll(PollMessagesRequest pollMessagesRequest);
}
