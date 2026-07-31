package io.saga.caravan.messaging;

import lombok.Builder;

@Builder
public record MessageBatchDeletionProperties(int maxDeleteBatchSize,
                                             int periodSeconds,
                                             int concurrency) {

  public MessageBatchDeletionProperties {
    if (maxDeleteBatchSize < 1) {
      throw new IllegalArgumentException("maxDeleteBatchSize must be greater or equal to 1");
    }

    if (periodSeconds < 1) {
      throw new IllegalArgumentException("periodSeconds must be greater or equal to 1");
    }

    if (concurrency < 1) {
      throw new IllegalArgumentException("concurrency must be greater or equal to 1");
    }
  }
}
