package io.saga.caravan.messaging;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class MessageDeletionBatcherTest {

  private static Message message(String id) {
    return new Message(id, "body-" + id, Map.of());
  }

  private static MessageBatchDeletionProperties propertiesOf(int maxDeleteBatchSize,
                                                             int deletionPeriodSeconds,
                                                             int deletionParallelism) {
    return MessageBatchDeletionProperties.builder()
        .maxDeleteBatchSize(maxDeleteBatchSize)
        .deletionPeriodSeconds(deletionPeriodSeconds)
        .deletionParallelism(deletionParallelism)
        .build();
  }

  @Nested
  class Batching {

    @Test
    void deliversFullBatchAsSoonAsMaxSizeIsReachedWithoutWaitingForThePeriod() {
      MessagesDeleter messagesDeleter = mock(MessagesDeleter.class);
      var batcher = new MessageDeletionBatcher(
          "queue", messagesDeleter, 10,
          propertiesOf(3, 30, 1));

      Message first = message("1");
      Message second = message("2");
      Message third = message("3");
      batcher.enqueueDeletion(first);
      batcher.enqueueDeletion(second);
      batcher.enqueueDeletion(third);

      try {
        await().atMost(Duration.ofSeconds(2))
            .untilAsserted(() -> verify(messagesDeleter).delete(List.of(first, second, third)));
      } finally {
        batcher.shutdownNow();
      }
    }

    @Test
    void deletesPartialBatchOncePeriodElapses() {
      MessagesDeleter messagesDeleter = mock(MessagesDeleter.class);
      var batcher = new MessageDeletionBatcher(
          "queue", messagesDeleter, 10,
          propertiesOf(10, 1, 1));

      Message message = message("1");
      batcher.enqueueDeletion(message);

      try {
        await().atMost(Duration.ofSeconds(3))
            .untilAsserted(() -> verify(messagesDeleter).delete(List.of(message)));
      } finally {
        batcher.shutdownNow();
      }
    }

    @Test
    void splitsMoreMessagesThanMaxBatchSizeAcrossMultipleBatches() {
      MessagesDeleter messagesDeleter = mock(MessagesDeleter.class);
      var batcher = new MessageDeletionBatcher(
          "queue", messagesDeleter, 10,
          propertiesOf(2, 1, 1));

      List<Message> messages = IntStream.rangeClosed(1, 5)
          .mapToObj(i -> message(String.valueOf(i)))
          .toList();
      messages.forEach(batcher::enqueueDeletion);

      try {
        await().atMost(Duration.ofSeconds(3))
            .untilAsserted(() -> verify(messagesDeleter, times(3)).delete(anyList()));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<Message>> batchCaptor = ArgumentCaptor.forClass(List.class);
        verify(messagesDeleter, times(3)).delete(batchCaptor.capture());

        List<List<Message>> batches = batchCaptor.getAllValues();
        assertThat(batches).allSatisfy(batch -> assertThat(batch.size()).isLessThanOrEqualTo(2));
        assertThat(batches.stream().flatMap(List::stream).toList())
            .containsExactlyElementsOf(messages);
      } finally {
        batcher.shutdownNow();
      }
    }
  }

  @Nested
  class FailureHandling {

    @Test
    void continuesDeletingSubsequentBatchesAfterTheDeleterThrows() {
      MessagesDeleter messagesDeleter = mock(MessagesDeleter.class);
      doThrow(new RuntimeException("boom"))
          .doNothing()
          .when(messagesDeleter).delete(anyList());

      var batcher = new MessageDeletionBatcher(
          "queue", messagesDeleter, 10,
          propertiesOf(1, 1, 1));

      Message failing = message("1");
      Message succeeding = message("2");
      batcher.enqueueDeletion(failing);
      batcher.enqueueDeletion(succeeding);

      try {
        await().atMost(Duration.ofSeconds(3))
            .untilAsserted(() -> {
              verify(messagesDeleter).delete(List.of(failing));
              verify(messagesDeleter).delete(List.of(succeeding));
            });
      } finally {
        batcher.shutdownNow();
      }
    }
  }

  @Nested
  class Parallelism {

    @Test
    void deletesMultipleBatchesConcurrentlyWhenParallelismIsGreaterThanOne() throws InterruptedException {
      CountDownLatch bothBatchesStarted = new CountDownLatch(2);
      CountDownLatch deleteThreadBlocker = new CountDownLatch(1);
      MessagesDeleter messagesDeleter = (_) -> {
        bothBatchesStarted.countDown();
        try {
          deleteThreadBlocker.await();
        } catch (InterruptedException exception) {
          Thread.currentThread().interrupt();
        }
      };

      var batcher = new MessageDeletionBatcher(
          "queue", messagesDeleter, 10,
          propertiesOf(1, 30, 2));

      batcher.enqueueDeletion(message("1"));
      batcher.enqueueDeletion(message("2"));

      try {
        assertThat(bothBatchesStarted.await(2, TimeUnit.SECONDS))
            .as("both single-message batches should be deleted concurrently instead of queued behind each other")
            .isTrue();
      } finally {
        deleteThreadBlocker.countDown();
        batcher.shutdownNow();
      }
    }

    @Test
    void keepsBatchesFullSizedEvenWithMultipleDeletionWorkers() {
      MessagesDeleter messagesDeleter = mock(MessagesDeleter.class);
      var batcher = new MessageDeletionBatcher(
          "queue", messagesDeleter, 30,
          propertiesOf(10, 5, 4));

      List<Message> messages = IntStream.rangeClosed(1, 20)
          .mapToObj(i -> message(String.valueOf(i)))
          .toList();
      messages.forEach(batcher::enqueueDeletion);

      try {
        await().atMost(Duration.ofSeconds(3))
            .untilAsserted(() -> verify(messagesDeleter, times(2)).delete(anyList()));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<Message>> batchCaptor = ArgumentCaptor.forClass(List.class);
        verify(messagesDeleter, times(2)).delete(batchCaptor.capture());

        assertThat(batchCaptor.getAllValues())
            .as("competing deletion workers must not fragment batches below the configured max size")
            .allSatisfy(batch -> assertThat(batch).hasSize(10));
      } finally {
        batcher.shutdownNow();
      }
    }
  }

  @Nested
  class Shutdown {

    @Test
    void isShutdownReflectsTheExecutorState() {
      var batcher = new MessageDeletionBatcher(
          "queue", mock(MessagesDeleter.class), 10,
          propertiesOf(10, 30, 1));

      assertThat(batcher.isShutdown()).isFalse();
      batcher.shutdown();
      assertThat(batcher.isShutdown()).isTrue();
    }

    @Test
    void drainsPendingMessagesOnGracefulShutdownEvenBeforeThePeriodElapses() throws InterruptedException {
      MessagesDeleter messagesDeleter = mock(MessagesDeleter.class);
      var batcher = new MessageDeletionBatcher(
          "queue", messagesDeleter, 10,
          propertiesOf(10, 30, 1));

      Message message = message("1");
      batcher.enqueueDeletion(message);
      batcher.shutdown();

      await().atMost(Duration.ofSeconds(3))
          .untilAsserted(() -> verify(messagesDeleter).delete(List.of(message)));
      assertThat(batcher.awaitTermination(Duration.ofSeconds(3))).isTrue();
    }

    @Test
    void shutdownNowInterruptsBlockedPollingPromptlyWithoutDraining() throws InterruptedException {
      MessagesDeleter messagesDeleter = mock(MessagesDeleter.class);
      var batcher = new MessageDeletionBatcher(
          "queue", messagesDeleter, 10,
          propertiesOf(10, 30, 1));

      batcher.shutdownNow();

      assertThat(batcher.awaitTermination(Duration.ofSeconds(2))).isTrue();
      verify(messagesDeleter, never()).delete(any());
    }
  }
}
