package io.saga.caravan.messaging;

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
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Slf4j
class MessageDeletionBatcher {

  private static final int MAX_DELETE_BATCH_SIZE = 10;
  private static final int WAIT_TIME_FOR_BATCH_TO_FILL_SECONDS = 1;

  private final String queueName;
  private final DeleteMessages deleteMessages;
  private final BlockingQueue<Message> pendingDeletions;
  private final ExecutorService deletionExecutorService;

  private volatile boolean shutdownRequested;

  MessageDeletionBatcher(String queueName, DeleteMessages deleteMessages) {
    this.queueName = queueName;
    this.deleteMessages = deleteMessages;
    this.pendingDeletions = new LinkedBlockingQueue<>();
    this.deletionExecutorService = createNewDeletionExecutorService(queueName);
    this.deletionExecutorService.execute(this::deleteContinuously);
  }

  private static ExecutorService createNewDeletionExecutorService(String queueName) {
    return Executors.newSingleThreadExecutor(
        Thread.ofVirtual().name("delete-" + queueName).factory());
  }

  void enqueueDeletion(Message message) {
    pendingDeletions.add(message);
  }

  private void deleteContinuously() {
    while ((!shutdownRequested || !pendingDeletions.isEmpty()) && !Thread.currentThread().isInterrupted()) {
      List<Message> batch = awaitNextBatch();
      if (!batch.isEmpty()) {
        attemptBatchDeletion(batch);
      }
    }
  }

  private List<Message> awaitNextBatch() {
    List<Message> batch = new ArrayList<>(MAX_DELETE_BATCH_SIZE);
    try {
      Instant deadline = Instant.now().plusSeconds(WAIT_TIME_FOR_BATCH_TO_FILL_SECONDS);
      do {
        Instant now = Instant.now();
        Message nextMessage = pollPendingDeletion(deadline.isAfter(now) ? Duration.between(now, deadline) : Duration.ZERO);
        if (nextMessage == null) {
          return batch;
        }
        batch.add(nextMessage);
      } while (batch.size() < MAX_DELETE_BATCH_SIZE);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
    }
    return batch;
  }

  private @Nullable Message pollPendingDeletion(Duration waitDuration) throws InterruptedException {
    return shutdownRequested || !waitDuration.isPositive()
        ? pendingDeletions.poll()
        : pendingDeletions.poll(waitDuration.toMillis(), MILLISECONDS);
  }

  private void attemptBatchDeletion(List<Message> batch) {
    try {
      deleteMessages.accept(batch);
    } catch (Exception exception) {
      log.warn(
          "Exception happened when deleting batch of {} messages from queueName={}",
          batch.size(), queueName, exception);
    }
  }

  boolean isShutdown() {
    return deletionExecutorService.isShutdown();
  }

  void shutdown() {
    shutdownRequested = true;
    deletionExecutorService.shutdown();
  }

  void shutdownNow() {
    shutdownRequested = true;
    deletionExecutorService.shutdownNow();
  }

  boolean awaitTermination(Duration timeout) throws InterruptedException {
    return deletionExecutorService.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }
}
