package io.saga.caravan.queue.polling;

import lombok.Builder;

@Builder
public record PollMessagesRequest(int numberOfMessages,
                                  int waitForSeconds) {
}
