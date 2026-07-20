package io.saga.caravan.messaging;

import java.util.Collection;
import java.util.function.Function;

public interface MessagesPoller extends Function<PollMessagesRequest, Collection<Message>> {
}
