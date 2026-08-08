package dev.baitursinov.caravan.queue.polling;

import lombok.Builder;

import static java.util.Objects.requireNonNull;

/**
 * Configures how a {@link ContinuousMessagePollingController} polls and processes a single
 * queue.
 *
 * @param concurrency                    the maximum number of messages processed concurrently;
 *                                       *                                   pollers make next poll once throughput of minPollSize gets free
 * @param maxPollSize                    the maximum number of messages requested in a single poll
 * @param minPollSize                    the minimum number of messages worth requesting in a
 *                                       single poll; polling waits for at least this much free
 *                                       processing throughput before issuing a request
 * @param pollersCountCap                the maximum number of concurrent poller threads; 0 for
 *                                       no cap beyond what {@code concurrency}/{@code maxPollSize}
 *                                       implies
 * @param pollWaitSeconds                how long a single poll request waits for messages to
 *                                       become available
 * @param messageBatchDeletionProperties how consumed messages are batched for deletion
 */
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

  /**
   * The maximum number of poller threads implied by {@code concurrency} and {@code maxPollSize},
   * capped by {@code pollersCountCap} if set.
   */
  public int maxPollersCount() {
    var divisionResult = Math.ceilDiv(concurrency, maxPollSize);
    if (pollersCountCap == 0) {
      return divisionResult;
    }
    return Math.min(pollersCountCap, divisionResult);
  }
}