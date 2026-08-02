package io.saga.caravan.autoconfigure.sqs;

import io.saga.caravan.queue.polling.MessageBatchDeletionProperties;
import io.saga.caravan.queue.polling.QueuePollingProperties;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.util.Set;

@ConfigurationProperties(CaravanQueuePollingConfigurationProperties.PREFIX)
public record CaravanQueuePollingConfigurationProperties(@Nullable String queueNamePrefix,
                                                         @DefaultValue("10") int gracefulShutdownSeconds,
                                                         @DefaultValue("10") int concurrency,
                                                         @DefaultValue("10") int maxPollSize,
                                                         @DefaultValue("3") int minPollSize,
                                                         @DefaultValue("0") int pollersCountCap,
                                                         @DefaultValue("10") int pollWaitSeconds,
                                                         @DefaultValue DeletionConfigurationProperties deletion,
                                                         @Nullable Set<String> subscribedEntities) {

  public static final String PREFIX = "caravan.event.messaging";

  public record DeletionConfigurationProperties(@DefaultValue("10") int maxBatchSize,
                                                @DefaultValue("1") int periodSeconds,
                                                @DefaultValue("3") int concurrency) {

  }

  public QueuePollingProperties toMessagingProperties() {
    return QueuePollingProperties.builder()
        .concurrency(concurrency)
        .maxPollSize(maxPollSize)
        .minPollSize(minPollSize)
        .pollersCountCap(pollersCountCap)
        .pollWaitSeconds(pollWaitSeconds)
        .messageBatchDeletionProperties(
            MessageBatchDeletionProperties.builder()
                .maxBatchSize(deletion.maxBatchSize())
                .periodSeconds(deletion.periodSeconds())
                .concurrency(deletion.concurrency())
                .build())
        .build();
  }
}
