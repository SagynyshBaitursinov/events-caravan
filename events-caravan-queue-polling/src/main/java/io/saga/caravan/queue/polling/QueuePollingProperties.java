package io.saga.caravan.queue.polling;

import lombok.Builder;

import static java.util.Objects.requireNonNull;

@Builder
public record QueuePollingProperties(int concurrency,
                                     int maxPollSize,
                                     int minPollSize,
                                     int pollersCountCap,
                                     int pollWaitSeconds,
                                     MessageBatchDeletionProperties messageBatchDeletionProperties) {

  public QueuePollingProperties {
    requireNonNull(messageBatchDeletionProperties, "messageBatchDeletionProperties must not be null");

    if (concurrency < 1) {
      throw new QueuePollingSetupException("concurrency must be greater or equal to 1");
    }

    if (maxPollSize < 1) {
      throw new QueuePollingSetupException("maxPollSize must be greater or equal to 1");
    }

    if (minPollSize < 1) {
      throw new QueuePollingSetupException("minPollSize must be greater or equal to 1");
    }

    if (pollersCountCap < 0) {
      throw new QueuePollingSetupException("pollersCountCap cannot be negative");
    }

    if (minPollSize > maxPollSize) {
      throw new QueuePollingSetupException("minPollSize cannot be greater than maxPollSize");
    }

    if (minPollSize > concurrency) {
      throw new QueuePollingSetupException("minPollSize cannot be greater than concurrency");
    }

    if (pollWaitSeconds < 1) {
      throw new QueuePollingSetupException("pollWaitSeconds must be greater or equal to 1");
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