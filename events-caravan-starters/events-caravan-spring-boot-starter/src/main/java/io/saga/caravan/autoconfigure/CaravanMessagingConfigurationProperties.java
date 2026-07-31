package io.saga.caravan.autoconfigure;

import io.saga.caravan.messaging.MessageBatchDeletionProperties;
import io.saga.caravan.messaging.MessagingProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(CaravanMessagingConfigurationProperties.PREFIX)
public record CaravanMessagingConfigurationProperties(String queueNamePrefix,
                                                      @DefaultValue("10") int concurrency,
                                                      @DefaultValue("10") int maxPollSize,
                                                      @DefaultValue("3") int minPollSize,
                                                      @DefaultValue("0") int pollersCountCap,
                                                      @DefaultValue("10") int pollWaitSeconds,
                                                      @DefaultValue("10") int gracefulShutdownSeconds,
                                                      @DefaultValue DeletionConfigurationProperties deletion) {

  public static final String PREFIX = "caravan.event.messaging";

  public record DeletionConfigurationProperties(@DefaultValue("10") int maxBatchSize,
                                                @DefaultValue("1") int periodSeconds,
                                                @DefaultValue("3") int concurrency) {

  }

  public MessagingProperties toMessagingProperties() {
    return MessagingProperties.builder()
        .concurrency(concurrency)
        .maxPollSize(maxPollSize)
        .minPollSize(minPollSize)
        .pollersCountCap(pollersCountCap)
        .pollWaitSeconds(pollWaitSeconds)
        .messageBatchDeletionProperties(
            MessageBatchDeletionProperties.builder()
                .maxDeleteBatchSize(deletion.maxBatchSize())
                .periodSeconds(deletion.periodSeconds())
                .concurrency(deletion.concurrency())
                .build())
        .build();
  }
}
