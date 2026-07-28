package io.saga.caravan.messaging;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MessagingConfiguration {

  @Bean
  public MessagingConfigurationProperties messagingApplicationProperties(
      @Value("${caravan.messaging.concurrency:10}") int concurrency,
      @Value("${caravan.messaging.max-poll-size:10}") int maxPollSize,
      @Value("${caravan.messaging.min-poll-size:1}") int minPollSize,
      @Value("${caravan.messaging.pollers-count-cap:0}") int pollersCountCap,
      @Value("${caravan.messaging.poll-wait-seconds:20}") int pollWaitSeconds,
      @Value("${caravan.messaging.graceful-shutdown-seconds:30}") int gracefulShutdownSeconds,
      @Value("${caravan.messaging.deletion.max-batch-size:10}") int maxDeleteBatchSize,
      @Value("${caravan.messaging.deletion.period-seconds:1}") int deletionPeriodSeconds,
      @Value("${caravan.messaging.deletion.parallelism:1}") int deletionParallelism) {

    return MessagingConfigurationProperties.builder()
        .concurrency(concurrency)
        .maxPollSize(maxPollSize)
        .minPollSize(minPollSize)
        .pollersCountCap(pollersCountCap)
        .pollWaitSeconds(pollWaitSeconds)
        .gracefulShutdownSeconds(gracefulShutdownSeconds)
        .deletionConfigurationProperties(
            MessagingConfigurationProperties.DeletionConfigurationProperties.builder()
                .maxDeleteBatchSize(maxDeleteBatchSize)
                .deletionPeriodSeconds(deletionPeriodSeconds)
                .deletionParallelism(deletionParallelism)
                .build())
        .build();
  }

  @Bean
  public MessagingProperties messagingProperties(MessagingConfigurationProperties properties) {
    var deletionConfigurationProperties = properties.deletionConfigurationProperties();
    return MessagingProperties.builder()
        .concurrency(properties.concurrency())
        .maxPollSize(properties.maxPollSize())
        .minPollSize(properties.minPollSize())
        .pollersCountCap(properties.pollersCountCap())
        .pollWaitSeconds(properties.pollWaitSeconds())
        .gracefulShutdownSeconds(properties.gracefulShutdownSeconds())
        .messageBatchDeletionProperties(
            MessageBatchDeletionProperties.builder()
                .maxDeleteBatchSize(deletionConfigurationProperties.maxDeleteBatchSize())
                .deletionPeriodSeconds(deletionConfigurationProperties.deletionPeriodSeconds())
                .deletionParallelism(deletionConfigurationProperties.deletionParallelism())
                .build())
        .build();
  }
}
