package io.saga.caravan.queue.polling;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ProcessingThroughputControllerTest {

  @Nested
  class ImmediateAcquisition {

    @Test
    void acquiresUpToMaxPollSizeWhenEnoughCapacityAvailable() {
      Semaphore semaphore = new Semaphore(5);
      var acquirer = new ProcessingThroughputController(semaphore, 2, 3, Duration.ofMillis(50));

      int acquired = acquirer.acquireThroughput();

      assertThat(acquired).isEqualTo(3);
      assertThat(semaphore.availablePermits()).isEqualTo(2);
    }

    @Test
    void acquiresAllAvailableCapacityWhenFewerThanMaxPollSizeButAtLeastMinPollSize() {
      Semaphore semaphore = new Semaphore(2);
      var capacityController = new ProcessingThroughputController(semaphore, 1, 5, Duration.ofMillis(50));

      int acquired = capacityController.acquireThroughput();

      assertThat(acquired).isEqualTo(2);
      assertThat(semaphore.availablePermits()).isZero();
    }
  }

  @Nested
  class BlockingAcquisition {

    @Test
    void acquiresMinPollSizeAfterWaitingForPermitsToBecomeAvailable() throws InterruptedException {
      Semaphore semaphore = new Semaphore(0);
      var acquirer = new ProcessingThroughputController(semaphore, 2, 5, Duration.ofMillis(500));

      Thread releaser = new Thread(() -> {
        try {
          Thread.sleep(30);
        } catch (InterruptedException exception) {
          Thread.currentThread().interrupt();
          return;
        }
        semaphore.release(2);
      });
      releaser.start();

      try {
        int acquired = acquirer.acquireThroughput();

        assertThat(acquired).isEqualTo(2);
        assertThat(semaphore.availablePermits()).isZero();
      } finally {
        releaser.join();
      }
    }

    @Test
    void returnsZeroWhenNoPermitsBecomeAvailableWithinTheWaitDuration() {
      Semaphore semaphore = new Semaphore(0);
      var acquirer = new ProcessingThroughputController(semaphore, 2, 5, Duration.ofMillis(30));

      int acquired = acquirer.acquireThroughput();

      assertThat(acquired).isZero();
      assertThat(semaphore.availablePermits()).isZero();
    }
  }

  @Nested
  class Interruption {

    @Test
    void returnsZeroAndPreservesInterruptStatusWhenInterruptedWhileWaiting() throws InterruptedException {
      Semaphore semaphore = new Semaphore(0);
      var acquirer = new ProcessingThroughputController(semaphore, 2, 5, Duration.ofSeconds(5));
      AtomicInteger result = new AtomicInteger(-1);
      AtomicBoolean interruptedAfterward = new AtomicBoolean(false);

      Thread worker = new Thread(() -> {
        result.set(acquirer.acquireThroughput());
        interruptedAfterward.set(Thread.currentThread().isInterrupted());
      });
      worker.start();
      Thread.sleep(50);
      worker.interrupt();
      worker.join(2000);

      assertThat(result.get()).isZero();
      assertThat(interruptedAfterward.get()).isTrue();
    }
  }
}
