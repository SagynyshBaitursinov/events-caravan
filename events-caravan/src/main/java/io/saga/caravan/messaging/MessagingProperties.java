package io.saga.caravan.messaging;

import lombok.Builder;

import static java.util.Objects.requireNonNull;

@Builder
public record MessagingProperties(int concurrency,
                                  int maxPollSize,
                                  int minPollSize,
                                  int pollersCountCap,
                                  int pollWaitSeconds,
                                  MessageBatchDeletionProperties messageBatchDeletionProperties) {

  public MessagingProperties {
    requireNonNull(messageBatchDeletionProperties, "messageBatchDeletionProperties must not be null");

    if (concurrency < 1) {
      throw new IllegalArgumentException("concurrency must be greater or equal to 1");
    }

    if (maxPollSize < 1) {
      throw new IllegalArgumentException("maxPollSize must be greater or equal to 1");
    }

    if (minPollSize < 1) {
      throw new IllegalArgumentException("minPollSize must be greater or equal to 1");
    }

    if (pollersCountCap < 0) {
      throw new IllegalArgumentException("pollersCountCap cannot be negative");
    }

    if (minPollSize > maxPollSize) {
      throw new IllegalArgumentException("minPollSize cannot be greater than maxPollSize");
    }

    if (minPollSize > concurrency) {
      throw new IllegalArgumentException("minPollSize cannot be greater than concurrency");
    }

    if (pollWaitSeconds < 1) {
      throw new IllegalArgumentException("pollWaitSeconds must be greater or equal to 1");
    }
  }

  public int maxPollersCount() {
    var divisionResult = Math.ceilDiv(concurrency, maxPollSize);
    if (pollersCountCap == 0) {
      return divisionResult;
    }
    return Math.min(pollersCountCap, divisionResult);
  }
}