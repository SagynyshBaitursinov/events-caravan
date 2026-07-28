package io.saga.caravan.messaging;

import lombok.Builder;

@Builder
public record MessagingConfigurationProperties(int concurrency,
                                               int maxPollSize,
                                               int minPollSize,
                                               int pollersCountCap,
                                               int pollWaitSeconds,
                                               int gracefulShutdownSeconds,
                                               DeletionConfigurationProperties deletionConfigurationProperties) {

  @Builder
  record DeletionConfigurationProperties(int maxDeleteBatchSize,
                                         int deletionPeriodSeconds,
                                         int deletionParallelism) {

  }
}
