package io.saga.caravan.messaging;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class PollingExecutor {

  private final ExecutorService executor;
  private final AtomicInteger activePollersCount;

  public PollingExecutor(String queueName) {
    this.executor = createExecutorService(queueName);
    this.activePollersCount = new AtomicInteger();
  }

  private ExecutorService createExecutorService(String queueName) {
    return Executors.newThreadPerTaskExecutor(
        Thread.ofPlatform()
            .name("poll-" + queueName + "-", 0)
            .daemon(false)
            .factory());
  }

  public Optional<Future<?>> submit(int maxPollersCount, Runnable runnable) {
    while (true) {
      int current = activePollersCount.get();
      if (current >= maxPollersCount) {
        return Optional.empty();
      }
      if (activePollersCount.compareAndSet(current, current + 1)) {
        break;
      }
    }

    try {
      return Optional.of(
          executor.submit(() -> {
            try {
              runnable.run();
            } finally {
              activePollersCount.decrementAndGet();
            }
          }));
    } catch (Exception exception) {
      activePollersCount.decrementAndGet();
      throw exception;
    }
  }

  public boolean isShutdown() {
    return executor.isShutdown();
  }

  public void shutdown() {
    executor.shutdown();
  }

  public void shutdownNow() {
    executor.shutdownNow();
  }

  public boolean awaitTermination(Duration timeout) throws InterruptedException {
    return executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }
}