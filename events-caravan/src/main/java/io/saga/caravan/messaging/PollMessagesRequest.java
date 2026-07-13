package io.saga.caravan.messaging;

import lombok.Builder;

@Builder
public record PollMessagesRequest(int numberOfMessages,
                                  int waitForSeconds) {
}
