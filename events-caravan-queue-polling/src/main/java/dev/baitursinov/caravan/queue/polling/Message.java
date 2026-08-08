package dev.baitursinov.caravan.queue.polling;

import java.util.Map;

/**
 * A message polled from a queue, transport-agnostic between {@link MessagesPoller},
 * {@link MessageConsumer} and {@link MessagesDeleter}.
 *
 * @param id       the message's unique identifier within the queue
 * @param body     the message's raw content
 * @param metadata transport-specific metadata needed to act on the message later, e.g. the
 *                 receipt handle an SQS {@link MessagesDeleter} needs to delete it
 */
public record Message(String id,
                      String body,
                      Map<String, String> metadata) {
}
