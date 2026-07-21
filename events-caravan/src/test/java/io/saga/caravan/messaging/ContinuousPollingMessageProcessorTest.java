package io.saga.caravan.messaging;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ContinuousPollingMessageProcessorTest {

  public static final String TEST_QUEUE_NAME = "test-queue";

  private static Message message(String id) {
    return new Message(id, "body-" + id, Map.of("meta", "data"));
  }

  private static MessagingProperties propertiesFor() {
    return MessagingProperties.builder()
        .concurrency(2)
        .maxPollSize(2)
        .minPollSize(1)
        .pollersCountCap(0)
        .pollWaitSeconds(1)
        .gracefulShutdownSeconds(5)
        .messageBatchDeletionProperties(
            MessageBatchDeletionProperties.builder()
                .maxDeleteBatchSize(1)
                .deletionPeriodSeconds(1)
                .deletionParallelism(2)
                .build())
        .build();
  }

  private static ContinuousPollingMessageProcessor processorWith(MessagingProperties properties,
                                                                 MessagesPoller messagesPoller,
                                                                 MessageConsumer messageConsumer,
                                                                 MessagesDeleter messagesDeleter) {
    return ContinuousPollingMessageProcessor.builder()
        .messagingProperties(properties)
        .queueName(TEST_QUEUE_NAME)
        .messagesPoller(messagesPoller)
        .messageConsumer(messageConsumer)
        .messagesDeleter(messagesDeleter)
        .build();
  }

  private static void stop(ContinuousPollingMessageProcessor processor) {
    processor.requestStopOfContinuousPolling();
    processor.awaitStopOfContinuousPolling(Instant.now().plusSeconds(5));
  }

  private static MessagesPoller emptyPollerWithShortDelay() {
    MessagesPoller poller = mock(MessagesPoller.class);
    when(poller.poll(any())).thenAnswer(_ -> List.of());
    return poller;
  }

  private static MessagesPoller pollerReturningOnceThenEmpty(List<Message> messages) {
    MessagesPoller poller = mock(MessagesPoller.class);
    AtomicBoolean delivered = new AtomicBoolean(false);

    when(poller.poll(any())).thenAnswer(_ -> {
      if (delivered.compareAndSet(false, true)) {
        return messages;
      }

      return List.of();
    });

    return poller;
  }

  @Nested
  class Processing {

    @Test
    void pollsConsumesAndDeletesPolledMessages() {
      Message message1 = message("1");
      Message message2 = message("2");
      var messages = List.of(message1, message2);
      MessagesPoller messagesPoller = pollerReturningOnceThenEmpty(messages);
      MessageConsumer messageConsumer = mock(MessageConsumer.class);
      MessagesDeleter messagesDeleter = mock(MessagesDeleter.class);
      var processor = processorWith(
          propertiesFor(), messagesPoller, messageConsumer, messagesDeleter);

      processor.startContinuousPolling();

      await().atMost(Duration.ofSeconds(3))
          .untilAsserted(() -> {
            verify(messageConsumer).consume(message1);
            verify(messageConsumer).consume(message2);
            verify(messagesDeleter).delete(List.of(message1));
          });

      stop(processor);
    }

    @Test
    void doesNotDeleteMessageWhenTheConsumerThrows() {
      var message = message("1");
      var messages = List.of(message);
      MessagesPoller messagesPoller = pollerReturningOnceThenEmpty(messages);
      MessageConsumer messageConsumer = mock(MessageConsumer.class);
      doThrow(new RuntimeException("boom")).when(messageConsumer).consume(message);
      MessagesDeleter messagesDeleter = mock(MessagesDeleter.class);
      var processor = processorWith(
          propertiesFor(), messagesPoller, messageConsumer, messagesDeleter);

      processor.startContinuousPolling();

      await().atMost(Duration.ofSeconds(3))
          .untilAsserted(() -> verify(messageConsumer).consume(message));

      verify(messagesDeleter, never()).delete(any());

      stop(processor);
    }
  }

  @Nested
  class Lifecycle {

    @Test
    void isNotRunningBeforeBeingStarted() {
      var processor = processorWith(
          propertiesFor(), emptyPollerWithShortDelay(), mock(MessageConsumer.class), mock(MessagesDeleter.class));

      assertThat(processor.isContinuousPollingRunning()).isFalse();
    }

    @Test
    void reportsRunningWhileStartedAndNotRunningOnceFullyStopped() {
      var processor = processorWith(
          propertiesFor(), emptyPollerWithShortDelay(), mock(MessageConsumer.class), mock(MessagesDeleter.class));

      processor.startContinuousPolling();
      assertThat(processor.isContinuousPollingRunning()).isTrue();

      stop(processor);
      assertThat(processor.isContinuousPollingRunning()).isFalse();
    }

    @Test
    void startingWhileAlreadyRunningShouldNotThrowException() {
      var processor = processorWith(
          propertiesFor(), emptyPollerWithShortDelay(), mock(MessageConsumer.class), mock(MessagesDeleter.class));

      processor.startContinuousPolling();

      assertThatCode(processor::startContinuousPolling).doesNotThrowAnyException();

      stop(processor);
    }

    @Test
    void requestingStopBeforeStartingShouldNotThrowException() {
      var processor = processorWith(
          propertiesFor(), emptyPollerWithShortDelay(), mock(MessageConsumer.class), mock(MessagesDeleter.class));

      assertThatCode(processor::requestStopOfContinuousPolling).doesNotThrowAnyException();
    }

    @Test
    void awaitingStopBeforeStartingDoesNotThrowException() {
      var processor = processorWith(
          propertiesFor(), emptyPollerWithShortDelay(), mock(MessageConsumer.class), mock(MessagesDeleter.class));

      assertThatCode(() -> processor.awaitStopOfContinuousPolling(Instant.now())).doesNotThrowAnyException();
    }

    @Test
    void canBeRestartedAfterBeingFullyStopped() {
      var processor = processorWith(
          propertiesFor(), emptyPollerWithShortDelay(), mock(MessageConsumer.class), mock(MessagesDeleter.class));

      processor.startContinuousPolling();
      stop(processor);
      assertThat(processor.isContinuousPollingRunning()).isFalse();

      processor.startContinuousPolling();

      assertThat(processor.isContinuousPollingRunning()).isTrue();

      stop(processor);
    }

    @Test
    void throwsWhenStartingAfterStopWasRequestedButNotYetAwaited() {
      CountDownLatch pollThreadBlocker = new CountDownLatch(1);
      MessagesPoller messagesPoller = mock(MessagesPoller.class);
      when(messagesPoller.poll(any())).thenAnswer(_ -> {
        pollThreadBlocker.await();
        return List.of();
      });

      var processor = processorWith(
          propertiesFor(), messagesPoller, mock(MessageConsumer.class), mock(MessagesDeleter.class));

      processor.startContinuousPolling();
      assertThat(processor.isContinuousPollingRunning()).isTrue();

      processor.requestStopOfContinuousPolling();

      assertThatThrownBy(processor::startContinuousPolling)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("stop was requested but not awaited");

      pollThreadBlocker.countDown();
    }
  }
}
