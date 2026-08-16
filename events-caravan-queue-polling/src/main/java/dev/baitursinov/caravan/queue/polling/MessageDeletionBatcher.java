package dev.baitursinov.caravan.queue.polling;

import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Slf4j
class MessageDeletionBatcher {

  private static final Duration WAITING_SLICE_MAX = Duration.ofMillis(100);

  private final String queueName;
  private final MessagesDeleter messagesDeleter;
  private final BlockingQueue<Message> pendingDeletions;
  private final MessageBatchDeletionProperties messageBatchDeletionProperties;
  private final Semaphore inParallelDeletions;
  private final ExecutorService pollingExecutorService;
  private final ExecutorService deletionExecutorService;

  private volatile boolean shutdownRequested;

  MessageDeletionBatcher(String queueName,
                         MessagesDeleter messagesDeleter,
                         int queueCapacity,
                         MessageBatchDeletionProperties messageBatchDeletionProperties) {
    this.queueName = queueName;
    this.messagesDeleter = messagesDeleter;
    this.pendingDeletions = new LinkedBlockingQueue<>(queueCapacity);
    this.messageBatchDeletionProperties = messageBatchDeletionProperties;
    this.inParallelDeletions = new Semaphore(messageBatchDeletionProperties.concurrency());
    this.deletionExecutorService = createNewDeletionExecutorService();
    this.pollingExecutorService = createNewPollingExecutorService();
  }

  void start() {
    pollingExecutorService.execute(this::pollContinuously);
  }

  private ExecutorService createNewDeletionExecutorService() {
    return Executors.newThreadPerTaskExecutor(
        Thread.ofVirtual().name("del-exec-" + queueName + "-", 0).factory());
  }

  private ExecutorService createNewPollingExecutorService() {
    return Executors.newSingleThreadExecutor(
        Thread.ofVirtual().name("del-poll-" + queueName).factory());
  }

  void enqueueDeletion(Message message) {
    try {
      log.debug("Enqueuing message with id={} for deletion", message.id());
      pendingDeletions.put(message);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
    }
  }

  private void pollContinuously() {
    while ((!shutdownRequested || !pendingDeletions.isEmpty()) && !Thread.currentThread().isInterrupted()) {
      List<Message> batch = awaitNextBatch();
      if (!batch.isEmpty()) {
        submitBatchForDeletion(batch);
      }
    }
    deletionExecutorService.shutdown();
  }

  private void submitBatchForDeletion(List<Message> batch) {
    try {
      inParallelDeletions.acquire();
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      return;
    }

    log.debug("Submitting batch of {} message(s) for deletion from queueName={}",
        batch.size(), queueName);
    try {
      deletionExecutorService.execute(() -> {
        try {
          messagesDeleter.delete(batch);
        } catch (Exception exception) {
          log.warn(
              "Exception happened when deleting batch of {} messages from queueName={}",
              batch.size(), queueName, exception);
        } finally {
          inParallelDeletions.release();
        }
      });
    } catch (RejectedExecutionException exception) {
      inParallelDeletions.release();
    }
  }

  private List<Message> awaitNextBatch() {
    int maxDeleteBatchSize = messageBatchDeletionProperties.maxBatchSize();
    List<Message> batch = new ArrayList<>(maxDeleteBatchSize);
    try {
      Instant deadline = Instant.now().plusSeconds(
          messageBatchDeletionProperties.periodSeconds());
      do {
        Message nextMessage = pollPendingDeletion(remainingUntil(deadline));
        if (nextMessage == null) {
          return batch;
        }
        batch.add(nextMessage);
      } while (batch.size() < maxDeleteBatchSize);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
    }
    return batch;
  }

  private @Nullable Message pollPendingDeletion(Duration waitDuration) throws InterruptedException {
    if (shutdownRequested || !waitDuration.isPositive()) {
      return pendingDeletions.poll();
    }

    return pollPendingDeletionInWaitingSlices(waitDuration);
  }

  private @Nullable Message pollPendingDeletionInWaitingSlices(Duration waitDuration) throws InterruptedException {
    Instant deadline = Instant.now().plus(waitDuration);
    while (true) {
      Duration remaining = remainingUntil(deadline);
      if (remaining.isZero()) {
        return null;
      }
      Duration waitingSlice = remaining.compareTo(WAITING_SLICE_MAX) < 0 ? remaining : WAITING_SLICE_MAX;
      Message message = pendingDeletions.poll(waitingSlice.toMillis(), MILLISECONDS);
      if (message != null || shutdownRequested) {
        return message;
      }
    }
  }

  private Duration remainingUntil(Instant deadline) {
    Instant now = Instant.now();
    return deadline.isAfter(now) ? Duration.between(now, deadline) : Duration.ZERO;
  }

  boolean isShutdown() {
    return pollingExecutorService.isShutdown();
  }

  void shutdown() {
    shutdownRequested = true;
    pollingExecutorService.shutdown();
  }

  void shutdownNow() {
    shutdownRequested = true;
    pollingExecutorService.shutdownNow();
    deletionExecutorService.shutdownNow();
  }

  boolean awaitTermination(Duration timeout) throws InterruptedException {
    Instant deadline = Instant.now().plus(timeout);
    boolean pollingTerminated = pollingExecutorService.awaitTermination(
        remainingMillis(deadline), MILLISECONDS);
    boolean deletionTerminated = deletionExecutorService.awaitTermination(
        remainingMillis(deadline), MILLISECONDS);
    return pollingTerminated && deletionTerminated;
  }

  private static long remainingMillis(Instant deadline) {
    return Math.max(0, Duration.between(Instant.now(), deadline).toMillis());
  }
}
