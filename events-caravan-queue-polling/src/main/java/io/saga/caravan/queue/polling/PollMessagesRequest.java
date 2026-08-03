package io.saga.caravan.queue.polling;

import lombok.Builder;

/**
 * Requests up to {@code numberOfMessages} messages from a {@link MessagesPoller}, waiting up to
 * {@code waitForSeconds} until at least one to become available.
 */
@Builder
public record PollMessagesRequest(int numberOfMessages,
                                  int waitForSeconds) {
}
