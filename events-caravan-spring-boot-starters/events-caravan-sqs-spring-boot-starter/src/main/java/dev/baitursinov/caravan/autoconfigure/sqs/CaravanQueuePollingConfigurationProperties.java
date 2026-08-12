package dev.baitursinov.caravan.autoconfigure.sqs;

import dev.baitursinov.caravan.queue.polling.MessageBatchDeletionProperties;
import dev.baitursinov.caravan.queue.polling.QueuePollingProperties;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.util.Set;

/**
 * Configures the SQS-backed queue polling autoconfigured under the {@value #PREFIX} prefix: one
 * queue per subscribed entity, each polled continuously per {@link #toMessagingProperties()}.
 *
 * @param queueNamePrefix         prefix prepended to each entity name to derive its queue name;
 * @param subscribedEntities      the entity names to create and poll a queue for; {@code null}
 *                                to subscribe to none
 * @param gracefulShutdownSeconds how long to wait for in-flight polling and processing to
 *                                finish when the application shuts down
 * @param concurrency             the maximum number of messages processed concurrently per queue;
 *                                pollers make next poll once throughput of minPollSize gets free
 * @param maxPollSize             the maximum number of messages requested in a single poll
 * @param minPollSize             the minimum number of messages worth requesting in a single poll
 * @param pollersCountCap         the maximum number of concurrent poller threads per queue; 0
 *                                for no cap beyond what {@code concurrency}/{@code maxPollSize}
 *                                implies
 * @param pollWaitSeconds         how long a single poll request waits for messages to become
 *                                available
 * @param deletion                how consumed messages are batched for deletion
 */
@ConfigurationProperties(CaravanQueuePollingConfigurationProperties.PREFIX)
public record CaravanQueuePollingConfigurationProperties(@Nullable String queueNamePrefix,
                                                         @Nullable Set<String> subscribedEntities,
                                                         @DefaultValue("10") int gracefulShutdownSeconds,
                                                         @DefaultValue("10") int concurrency,
                                                         @DefaultValue("10") int maxPollSize,
                                                         @DefaultValue("3") int minPollSize,
                                                         @DefaultValue("0") int pollersCountCap,
                                                         @DefaultValue("10") int pollWaitSeconds,
                                                         @DefaultValue DeletionConfigurationProperties deletion) {

  public static final String PREFIX = "caravan.event.messaging";

  /**
   * Configures how successfully consumed messages are batched before being deleted from their queue.
   *
   * @param maxBatchSize  the maximum number of messages deleted in a single batch
   * @param periodSeconds how long to wait for a batch to fill up to {@code maxBatchSize} before
   *                      deleting whatever has accumulated so far
   * @param concurrency   the maximum number of delete batches in flight at once
   */
  public record DeletionConfigurationProperties(@DefaultValue("10") int maxBatchSize,
                                                @DefaultValue("1") int periodSeconds,
                                                @DefaultValue("3") int concurrency) {

  }

  /**
   * Converts these properties to the transport-agnostic {@link QueuePollingProperties} used by
   * {@code events-caravan-queue-polling}.
   */
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
