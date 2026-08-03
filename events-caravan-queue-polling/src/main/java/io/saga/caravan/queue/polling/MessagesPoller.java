package io.saga.caravan.queue.polling;

import java.util.Collection;

/**
 * Polls messages from a queue. Implemented by extenders for a specific transport (e.g. SQS);
 * used by {@link ContinuousMessagePollingController} to fetch each batch of messages to process.
 */
public interface MessagesPoller {

  /**
   * Polls up to {@code pollMessagesRequest}'s requested number of messages, waiting up to its
   * requested time for at least one to become available. Returns an empty collection if none
   * are available within that time.
   */
  Collection<Message> poll(PollMessagesRequest pollMessagesRequest);
}
