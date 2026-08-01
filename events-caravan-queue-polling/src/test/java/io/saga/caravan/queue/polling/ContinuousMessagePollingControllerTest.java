package io.saga.caravan.queue.polling;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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

class ContinuousMessagePollingControllerTest {

  public static final String TEST_QUEUE_NAME = "test-queue";

  private static Message message(String id) {
    return new Message(id, "body-" + id, Map.of("meta", "data"));
  }

  private static QueuePollingProperties propertiesFor() {
    return QueuePollingProperties.builder()
        .concurrency(2)
        .maxPollSize(2)
        .minPollSize(1)
        .pollersCountCap(0)
        .pollWaitSeconds(1)
        .messageBatchDeletionProperties(
            MessageBatchDeletionProperties.builder()
                .maxBatchSize(1)
                .periodSeconds(1)
                .concurrency(2)
                .build())
        .build();
  }

  private static ContinuousMessagePollingController pollingController(QueuePollingProperties properties,
                                                                      MessagesPoller messagesPoller,
                                                                      MessageConsumer messageConsumer,
                                                                      MessagesDeleter messagesDeleter) {
    return ContinuousMessagePollingController.builder()
        .queuePollingProperties(properties)
        .queueName(TEST_QUEUE_NAME)
        .messagesPoller(messagesPoller)
        .messageConsumer(messageConsumer)
        .messagesDeleter(messagesDeleter)
        .build();
  }

  private static void stop(ContinuousMessagePollingController controller) {
    controller.requestStopOfContinuousPolling();
    controller.awaitStopOfContinuousPolling(Instant.now().plusSeconds(5));
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
      var controller = pollingController(
          propertiesFor(), messagesPoller, messageConsumer, messagesDeleter);

      controller.startContinuousPolling();

      await().atMost(Duration.ofSeconds(3))
          .untilAsserted(() -> {
            verify(messageConsumer).consume(message1);
            verify(messageConsumer).consume(message2);
            verify(messagesDeleter).delete(List.of(message1));
          });

      stop(controller);
    }

    @Test
    void doesNotDeleteMessageWhenTheConsumerThrows() {
      var message = message("1");
      var messages = List.of(message);
      MessagesPoller messagesPoller = pollerReturningOnceThenEmpty(messages);
      MessageConsumer messageConsumer = mock(MessageConsumer.class);
      doThrow(new RuntimeException("boom")).when(messageConsumer).consume(message);
      MessagesDeleter messagesDeleter = mock(MessagesDeleter.class);
      var controller = pollingController(
          propertiesFor(), messagesPoller, messageConsumer, messagesDeleter);

      controller.startContinuousPolling();

      await().atMost(Duration.ofSeconds(3))
          .untilAsserted(() -> verify(messageConsumer).consume(message));

      verify(messagesDeleter, never()).delete(any());

      stop(controller);
    }
  }

  @Nested
  class Lifecycle {

    @Test
    void isNotRunningBeforeBeingStarted() {
      var controller = pollingController(
          propertiesFor(), emptyPollerWithShortDelay(), mock(MessageConsumer.class), mock(MessagesDeleter.class));

      assertThat(controller.isContinuousPollingRunning()).isFalse();
    }

    @Test
    void reportsRunningWhileStartedAndNotRunningOnceFullyStopped() {
      var controller = pollingController(
          propertiesFor(), emptyPollerWithShortDelay(), mock(MessageConsumer.class), mock(MessagesDeleter.class));

      controller.startContinuousPolling();
      assertThat(controller.isContinuousPollingRunning()).isTrue();

      stop(controller);
      assertThat(controller.isContinuousPollingRunning()).isFalse();
    }

    @Test
    void startingWhileAlreadyRunningShouldNotThrowException() {
      var controller = pollingController(
          propertiesFor(), emptyPollerWithShortDelay(), mock(MessageConsumer.class), mock(MessagesDeleter.class));

      controller.startContinuousPolling();

      assertThatCode(controller::startContinuousPolling).doesNotThrowAnyException();

      stop(controller);
    }

    @Test
    void requestingStopBeforeStartingShouldNotThrowException() {
      var controller = pollingController(
          propertiesFor(), emptyPollerWithShortDelay(), mock(MessageConsumer.class), mock(MessagesDeleter.class));

      assertThatCode(controller::requestStopOfContinuousPolling).doesNotThrowAnyException();
    }

    @Test
    void awaitingStopBeforeStartingDoesNotThrowException() {
      var controller = pollingController(
          propertiesFor(), emptyPollerWithShortDelay(), mock(MessageConsumer.class), mock(MessagesDeleter.class));

      assertThatCode(() -> controller.awaitStopOfContinuousPolling(Instant.now())).doesNotThrowAnyException();
    }

    @Test
    void canBeRestartedAfterBeingFullyStopped() {
      var controller = pollingController(
          propertiesFor(), emptyPollerWithShortDelay(), mock(MessageConsumer.class), mock(MessagesDeleter.class));

      controller.startContinuousPolling();
      stop(controller);
      assertThat(controller.isContinuousPollingRunning()).isFalse();

      controller.startContinuousPolling();

      assertThat(controller.isContinuousPollingRunning()).isTrue();

      stop(controller);
    }

    @Test
    void throwsWhenStartingAfterStopWasRequestedButNotYetAwaited() throws InterruptedException {
      CountDownLatch pollEntered = new CountDownLatch(1);
      CountDownLatch pollThreadBlocker = new CountDownLatch(1);
      MessagesPoller messagesPoller = mock(MessagesPoller.class);
      when(messagesPoller.poll(any())).thenAnswer(_ -> {
        pollEntered.countDown();
        pollThreadBlocker.await();
        return List.of();
      });

      var controller = pollingController(
          propertiesFor(), messagesPoller, mock(MessageConsumer.class), mock(MessagesDeleter.class));

      controller.startContinuousPolling();

      assertThat(controller.isContinuousPollingRunning()).isTrue();
      assertThat(pollEntered.await(5, TimeUnit.SECONDS)).isTrue();

      controller.requestStopOfContinuousPolling();

      assertThatThrownBy(controller::startContinuousPolling)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("stop was requested but not awaited");

      pollThreadBlocker.countDown();
    }
  }
}
