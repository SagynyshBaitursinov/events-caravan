package io.saga.caravan.messaging;

import java.util.Collection;
import java.util.function.Function;

public interface PollMessages extends Function<PollMessagesRequest, Collection<Message>> {
}
