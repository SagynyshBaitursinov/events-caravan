package io.saga.caravan.messaging;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
public class ContinuousPollingMessageProcessor {

  private final ExecutorService pollingExecutor;
  private final Executor messageHandlingExecutor;
  private final MessagingProperties messagingProperties;
  private final String queueName;

  private final Function<PollMessagesRequest, Collection<Message>> pollMessages;
  private final Consumer<Message> consumeMessage;
  private final Consumer<Message> deleteMessage;

  private final Semaphore freeProcessingCapacity;

  private volatile boolean shouldKeepPolling;

  @Nullable
  private volatile Future<?> continuousPolling;

  @Builder
  public ContinuousPollingMessageProcessor(ExecutorService pollingExecutor,
                                           Executor messageHandlingExecutor,
                                           MessagingProperties messagingProperties,
                                           String queueName,
                                           Function<PollMessagesRequest, Collection<Message>> pollMessages,
                                           Consumer<Message> consumeMessage,
                                           Consumer<Message> deleteMessage) {
    this.pollingExecutor = requireNonNull(pollingExecutor);
    this.messageHandlingExecutor = requireNonNull(messageHandlingExecutor);
    this.messagingProperties = requireNonNull(messagingProperties);
    this.queueName = requireNonNull(queueName);
    this.pollMessages = requireNonNull(pollMessages);
    this.consumeMessage = requireNonNull(consumeMessage);
    this.deleteMessage = requireNonNull(deleteMessage);

    this.freeProcessingCapacity = new Semaphore(messagingProperties.concurrency());
  }

  public synchronized void startContinuousPolling() {
    shouldKeepPolling = true;

    if (isContinuousPollingRunning()) {
      return;
    }

    log.info("Starting to continuously poll messages from queueName={}", queueName);
    continuousPolling = pollingExecutor.submit(this::pollUntilStopped);
  }

  public boolean isContinuousPollingRunning() {
    var polling = continuousPolling;
    return polling != null && !polling.isDone();
  }

  private void pollUntilStopped() {
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

      if (polledMessages.size() < acquiredPermits) {
        freeProcessingCapacity.release(acquiredPermits - polledMessages.size());
      }

      polledMessages.forEach(this::process);
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
            messagingProperties.pollWaitSeconds(),
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
      SECONDS.sleep(messagingProperties.postPollingFailureWaitSeconds());
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
    if (continuousPolling == null) {
      log.info("Continuous polling of messages from queueName={} is already stopped", queueName);
      return;
    }

    log.info("Requesting stop of continuous polling of messages from queueName={}", queueName);
    shouldKeepPolling = false;
  }

  public synchronized void awaitStopOfContinuousPolling(Instant deadline) {
    var polling = continuousPolling;
    if (polling == null) {
      return;
    }

    awaitPollingLoopExit(polling, deadline);
    awaitInFlightMessageHandlers(deadline);

    continuousPolling = null;
  }

  private void awaitPollingLoopExit(Future<?> polling, Instant deadline) {
    try {
      long remainingMillis = Math.max(0, Duration.between(Instant.now(), deadline).toMillis());
      polling.get(remainingMillis, MILLISECONDS);
      log.info("Gracefully stopped continuous polling of messages from queueName={}", queueName);
    } catch (TimeoutException exception) {
      log.warn("Polling loop for queueName={} did not finish before shutdown deadline", queueName);
      polling.cancel(true);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      polling.cancel(true);
    } catch (ExecutionException exception) {
      log.warn("Polling loop for queueName={} terminated with an error", queueName, exception.getCause());
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
