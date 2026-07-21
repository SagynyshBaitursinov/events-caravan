package io.saga.caravan.messaging;

import java.util.Collection;

public interface MessagesPoller {

  Collection<Message> poll(PollMessagesRequest pollMessagesRequest);
}
