package io.saga.caravan.messaging;

import lombok.Builder;

@Builder
public record MessagingProperties(int concurrency,
                                  int maxPollSize,
                                  int minPollSize,
                                  int pollersCountCap,
                                  int pollWaitSeconds,
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

    if (pollersCountCap < 0) {
      throw new IllegalArgumentException("pollersCountCap cannot be negative");
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

    if (gracefulShutdownSeconds < 0) {
      throw new IllegalArgumentException("gracefulShutdownSeconds must be greater or equal to 0");
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