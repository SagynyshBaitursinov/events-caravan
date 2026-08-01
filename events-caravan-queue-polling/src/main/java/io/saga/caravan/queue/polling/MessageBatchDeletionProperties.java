package io.saga.caravan.queue.polling;

import lombok.Builder;

@Builder
public record MessageBatchDeletionProperties(int maxBatchSize,
                                             int periodSeconds,
                                             int concurrency) {

  public MessageBatchDeletionProperties {
    if (maxBatchSize < 1) {
      throw new QueuePollingSetupException("maxBatchSize must be greater or equal to 1");
    }

    if (periodSeconds < 1) {
      throw new QueuePollingSetupException("periodSeconds must be greater or equal to 1");
    }

    if (concurrency < 1) {
      throw new QueuePollingSetupException("concurrency must be greater or equal to 1");
    }
  }
}
