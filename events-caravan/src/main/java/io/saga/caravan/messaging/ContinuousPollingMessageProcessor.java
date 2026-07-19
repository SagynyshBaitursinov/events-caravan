package io.saga.caravan.messaging;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
public class ContinuousPollingMessageProcessor {

  private static final int SECONDS_TO_WAIT_BEFORE_CAPACITY_GETS_AVAILABLE = 1;
  private static final int SECONDS_TO_SLEEP_AFTER_FAILURE = 5;

  private final Executor messageHandlingExecutor;
  private final MessagingProperties messagingProperties;
  private final String queueName;

  private final PollMessages pollMessages;
  private final ConsumeMessage consumeMessage;
  private final DeleteMessage deleteMessage;

  private final Semaphore freeProcessingCapacity;
  private final int maxPollersCount;

  private volatile boolean shouldKeepPolling;
  private volatile ExecutorService pollingExecutor;
  @Nullable
  private volatile Future<?> primaryContinuousPoller;
  private final AtomicInteger activePollersCount = new AtomicInteger(1);

  @Builder
  public ContinuousPollingMessageProcessor(Executor messageHandlingExecutor,
                                           MessagingProperties messagingProperties,
                                           String queueName,
                                           PollMessages pollMessages,
                                           ConsumeMessage consumeMessage,
                                           DeleteMessage deleteMessage) {
    this.messageHandlingExecutor = requireNonNull(messageHandlingExecutor);
    this.messagingProperties = requireNonNull(messagingProperties);
    this.queueName = requireNonNull(queueName);
    this.pollMessages = requireNonNull(pollMessages);
    this.consumeMessage = requireNonNull(consumeMessage);
    this.deleteMessage = requireNonNull(deleteMessage);

    this.pollingExecutor = createNewExecutorService(queueName);
    this.freeProcessingCapacity = new Semaphore(messagingProperties.concurrency());
    this.maxPollersCount = messagingProperties.maxPollersCount();
  }

  private ExecutorService createNewExecutorService(String queueName) {
    return Executors.newThreadPerTaskExecutor(
        Thread.ofPlatform()
            .name("poller-" + queueName + "-", 0)
            .daemon(false)
            .factory());
  }

  public synchronized void startContinuousPolling() {
    if (isStopRequestedButNotAwaited()) {
      throw new IllegalStateException(
          ("Cannot start continuous polling of queueName=%s: stop was requested but not awaited, " +
              "call awaitStopOfContinuousPolling before restarting")
              .formatted(queueName));
    }

    shouldKeepPolling = true;

    if (isContinuousPollingRunning()) {
      return;
    }

    log.info("Starting to continuously poll messages from queueName={}", queueName);
    if (pollingExecutor.isShutdown()) {
      pollingExecutor = createNewExecutorService(queueName);
      activePollersCount.set(1);
    }
    primaryContinuousPoller = pollingExecutor.submit(
        () -> pollUntilStopped(true));
  }

  private boolean isStopRequestedButNotAwaited() {
    return !shouldKeepPolling && primaryContinuousPoller != null;
  }

  public boolean isContinuousPollingRunning() {
    var polling = primaryContinuousPoller;
    return polling != null && !polling.isDone();
  }

  private void pollUntilStopped(boolean isPrimaryPoller) {
    try {
      doPollUntilStopped(isPrimaryPoller);
    } catch (Exception exception) {
      log.warn("Polling loop for queueName={} terminated with an error", queueName, exception);
    }
  }

  private void doPollUntilStopped(boolean isPrimaryPoller) {
    while (shouldKeepPolling && !Thread.currentThread().isInterrupted()) {
      var acquiredPermits = acquirePermitsForPolling();
      if (acquiredPermits == 0) {
        continue;
      }
      if (!shouldKeepPolling) {
        freeProcessingCapacity.release(acquiredPermits);
        return;
      }

      Collection<Message> polledMessages = attemptMessagePolling(acquiredPermits);

      if (polledMessages.size() == messagingProperties.maxPollSize()) {
        spawnExtraPoller();
      } else if (!isPrimaryPoller && polledMessages.isEmpty()) {
        return;
      }

      if (polledMessages.size() < acquiredPermits) {
        freeProcessingCapacity.release(acquiredPermits - polledMessages.size());
      }

      polledMessages.forEach(this::process);
    }
  }

  private void spawnExtraPoller() {
    int currentActivePollersCount = activePollersCount.get();
    if (currentActivePollersCount < maxPollersCount
        && activePollersCount.compareAndSet(currentActivePollersCount, currentActivePollersCount + 1)) {

      pollingExecutor.submit(() -> {
        try {
          pollUntilStopped(false);
        } finally {
          activePollersCount.decrementAndGet();
        }
      });
    }
  }

  private Collection<Message> attemptMessagePolling(int acquiredPermits) {
    try {
      return pollMessages(acquiredPermits);
    } catch (Exception exception) {
      log.warn("Exception occurred when polling from queueName={}", queueName, exception);
      backOffPostPollingFailure();
      return Collections.emptyList();
    }
  }

  private Collection<Message> pollMessages(int acquiredPermits) {
    return pollMessages.apply(
        PollMessagesRequest.builder()
            .numberOfMessages(acquiredPermits)
            .waitForSeconds(messagingProperties.pollWaitSeconds())
            .build());
  }

  private int acquirePermitsForPolling() {
    int permitsTargetToAcquire = Math.min(
        freeProcessingCapacity.availablePermits(),
        messagingProperties.maxPollSize());

    if (permitsTargetToAcquire >= messagingProperties.minPollSize()
        && freeProcessingCapacity.tryAcquire(permitsTargetToAcquire)) {
      return permitsTargetToAcquire;
    } else {
      try {
        boolean acquired = freeProcessingCapacity.tryAcquire(
            messagingProperties.minPollSize(),
            SECONDS_TO_WAIT_BEFORE_CAPACITY_GETS_AVAILABLE,
            SECONDS);
        if (acquired) {
          return messagingProperties.minPollSize();
        } else {
          return 0;
        }
      } catch (InterruptedException interruptedException) {
        Thread.currentThread().interrupt();
        return 0;
      }
    }
  }

  private void process(Message message) {
    try {
      messageHandlingExecutor.execute(
          () -> consumeAndDeleteMessage(message));
    } catch (Exception exception) {
      log.warn("Exception occurred when processing message with id={}", message.id(), exception);
      freeProcessingCapacity.release();
    }
  }

  private void backOffPostPollingFailure() {
    try {
      SECONDS.sleep(SECONDS_TO_SLEEP_AFTER_FAILURE);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
    }
  }

  private void consumeAndDeleteMessage(Message message) {
    try {
      consumeMessage.accept(message);
      deleteMessage.accept(message);
    } catch (Exception exception) {
      log.warn(
          "Exception happened when processing message with id={} from queueName={}",
          message.id(), queueName, exception);
    } finally {
      freeProcessingCapacity.release();
    }
  }

  public synchronized void requestStopOfContinuousPolling() {
    if (primaryContinuousPoller == null) {
      log.info("Continuous polling of messages from queueName={} is already stopped", queueName);
      return;
    }

    log.info("Requesting stop of continuous polling of messages from queueName={}", queueName);
    shouldKeepPolling = false;
  }

  public synchronized void awaitStopOfContinuousPolling(Instant deadline) {
    if (primaryContinuousPoller == null) {
      return;
    }

    awaitPollingLoopExit(deadline);
    awaitInFlightMessageHandlers(deadline);

    primaryContinuousPoller = null;
    activePollersCount.set(0);
  }

  private void awaitPollingLoopExit(Instant deadline) {
    pollingExecutor.shutdown();
    try {
      long remainingMillis = Math.max(0, Duration.between(Instant.now(), deadline).toMillis());
      if (pollingExecutor.awaitTermination(remainingMillis, MILLISECONDS)) {
        log.info("Gracefully stopped continuous polling of messages from queueName={}", queueName);
      } else {
        log.warn("Polling loop for queueName={} did not finish before shutdown deadline", queueName);
        pollingExecutor.shutdownNow();
      }
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      pollingExecutor.shutdownNow();
    }
  }

  private void awaitInFlightMessageHandlers(Instant deadline) {
    int concurrency = messagingProperties.concurrency();
    long remainingMillis = Math.max(0, Duration.between(Instant.now(), deadline).toMillis());
    try {
      if (freeProcessingCapacity.tryAcquire(concurrency, remainingMillis, MILLISECONDS)) {
        freeProcessingCapacity.release(concurrency);
      } else {
        log.warn("In-flight messages processing for queueName={} did not finish before shutdown deadline", queueName);
      }
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
    }
  }
}
