package dev.baitursinov.caravan.queue.polling;

import lombok.RequiredArgsConstructor;

import java.time.Duration;
import java.util.concurrent.Semaphore;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@RequiredArgsConstructor
class ProcessingThroughputController {

  private final Semaphore freeProcessingThroughout;
  private final int minPollSize;
  private final int maxPollSize;
  private final Duration waitForThroughputDuration;

  int acquireThroughput() {
    int throughputTargetToAcquire = Math.min(
        freeProcessingThroughout.availablePermits(), maxPollSize);

    if (canAcquireThroughputImmediately(throughputTargetToAcquire)) {
      return throughputTargetToAcquire;
    }

    try {
      if (waitForThroughputAvailable(minPollSize)) {
        return minPollSize;
      } else {
        return 0;
      }
    } catch (InterruptedException interruptedException) {
      Thread.currentThread().interrupt();
      return 0;
    }
  }

  private boolean canAcquireThroughputImmediately(int throughputTargetToAcquire) {
    return throughputTargetToAcquire >= minPollSize
        && freeProcessingThroughout.tryAcquire(throughputTargetToAcquire);
  }

  private boolean waitForThroughputAvailable(int throughput) throws InterruptedException {
    return freeProcessingThroughout.tryAcquire(
        throughput,
        waitForThroughputDuration.toMillis(),
        MILLISECONDS);
  }

  public void release(int throughput) {
    freeProcessingThroughout.release(throughput);
  }
}
