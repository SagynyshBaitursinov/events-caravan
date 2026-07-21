package io.saga.caravan.messaging;

import lombok.Builder;

@Builder
public record MessageBatchDeletionProperties(int maxDeleteBatchSize,
                                             int deletionPeriodSeconds,
                                             int deletionParallelism) {

  public MessageBatchDeletionProperties {
    if (maxDeleteBatchSize < 1) {
      throw new IllegalArgumentException("maxDeleteBatchSize must be greater or equal to 1");
    }

    if (deletionPeriodSeconds < 1) {
      throw new IllegalArgumentException("deletionPeriodSeconds must be greater or equal to 1");
    }

    if (deletionParallelism < 1) {
      throw new IllegalArgumentException("deletionParallelism must be greater or equal to 1");
    }
  }
}
