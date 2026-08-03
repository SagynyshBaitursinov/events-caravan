package io.saga.caravan.queue.polling;

import lombok.Builder;

/**
 * Configures how successfully consumed messages are batched before being passed to a
 * {@link MessagesDeleter}, as part of {@link QueuePollingProperties}.
 *
 * @param maxBatchSize  the maximum number of messages deleted in a single batch
 * @param periodSeconds how long to wait for a batch to fill up to {@code maxBatchSize} before
 *                      deleting whatever has accumulated so far
 * @param concurrency   the maximum number of delete batches in flight at once
 */
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
