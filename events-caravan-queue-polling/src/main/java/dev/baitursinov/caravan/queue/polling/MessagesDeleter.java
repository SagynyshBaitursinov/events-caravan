package dev.baitursinov.caravan.queue.polling;

import java.util.List;

/**
 * Deletes already-consumed messages from a queue. Implemented by extenders for a specific
 * transport (e.g. SQS); used by {@link ContinuousMessagePollingController}, via an internal
 * batching layer, once a message has been successfully consumed.
 */
public interface MessagesDeleter {

  /**
   * Deletes the given messages from the queue. Implementations should tolerate partial
   * failures without throwing, since a message that fails to delete will simply be redelivered
   * and consumed again.
   */
  void delete(List<Message> messages);
}
