package io.saga.caravan.messaging;

import lombok.Builder;

@Builder
public record MessagingProperties(int concurrency,
                                  int maxPollSize,
                                  int minPollSize,
                                  int pollWaitSeconds,
                                  int postPollingFailureWaitSeconds,
                                  int gracefulShutdownSeconds) {

  public MessagingProperties {
    if (concurrency <= 0) {
      throw new IllegalArgumentException("concurrency must be greater than 0");
    }

    if (maxPollSize < 1) {
      throw new IllegalArgumentException("maxPollSize must be greater or equal to 1");
    }

    if (minPollSize < 1) {
      throw new IllegalArgumentException("minPollSize must be greater or equal to 1");
    }

    if (minPollSize > maxPollSize) {
      throw new IllegalArgumentException("minPollSize cannot be greater than maxPollSize");
    }

    if (minPollSize > concurrency) {
      throw new IllegalArgumentException("minPollSize cannot be greater than concurrency");
    }

    if (pollWaitSeconds <= 0) {
      throw new IllegalArgumentException("pollWaitSeconds must be greater than 0");
    }

    if (postPollingFailureWaitSeconds <= 0) {
      throw new IllegalArgumentException("postPollingFailureWaitSeconds must be greater than 0");
    }

    if (gracefulShutdownSeconds < 0) {
      throw new IllegalArgumentException("gracefulShutdownSeconds must be greater or equal to 0");
    }
  }
}