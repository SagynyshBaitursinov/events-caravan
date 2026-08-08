package dev.baitursinov.caravan.queue.polling;

/**
 * Processes a single polled message. Implemented by applications and given to a
 * {@link ContinuousMessagePollingController}, which invokes it for each message polled and
 * deletes the message only after this method returns without throwing a runtime exception.
 */
public interface MessageConsumer {

  /**
   * Processes the given message. Any exception prevents the message from being deleted, so it
   * will be redelivered.
   */
  void consume(Message message);
}
