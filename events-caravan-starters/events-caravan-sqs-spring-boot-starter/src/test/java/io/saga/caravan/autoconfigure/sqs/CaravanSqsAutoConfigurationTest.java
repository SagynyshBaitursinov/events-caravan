package io.saga.caravan.autoconfigure.sqs;

import io.saga.caravan.event.EntityEventsRegistration;
import io.saga.caravan.event.consumer.EventMessageConsumer;
import io.saga.caravan.queue.polling.MessageBatchDeletionProperties;
import io.saga.caravan.queue.polling.QueuePollingProperties;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CaravanSqsAutoConfigurationTest {

  private static final String QUEUE_URL = "http://localhost:4566/000000000000/test-app_calculator";

  AutoConfigurations autoConfigurations = AutoConfigurations.of(CaravanSqsAutoConfiguration.class);

  ApplicationContextRunner contextRunner = new ApplicationContextRunner()
      .withConfiguration(autoConfigurations)
      .withUserConfiguration(ApplicationConfiguration.class)
      .withPropertyValues("caravan.event.messaging.queue-name-prefix=test-app");

  @Configuration(proxyBeanMethods = false)
  static class ApplicationConfiguration {

    @Bean
    SqsClient sqsClient() {
      var sqsClient = mock(SqsClient.class);
      when(sqsClient.getQueueUrl(any(GetQueueUrlRequest.class)))
          .thenReturn(
              GetQueueUrlResponse.builder()
                  .queueUrl(QUEUE_URL)
                  .build());
      return sqsClient;
    }

    @Bean
    EventMessageConsumer eventMessageConsumer() {
      return mock(EventMessageConsumer.class);
    }

    @Bean
    EntityEventsRegistration calculatorEventsRegistration() {
      return new EntityEventsRegistration("calculator", Map.of(), true);
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class ApplicationConfigurationWithoutSqsClient {

    @Bean
    EventMessageConsumer eventMessageConsumer() {
      return mock(EventMessageConsumer.class);
    }

    @Bean
    EntityEventsRegistration calculatorEventsRegistration() {
      return new EntityEventsRegistration("calculator", Map.of(), true);
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class ApplicationConfigurationWithNoSubscribedQueues {

    @Bean
    SqsClient sqsClient() {
      return mock(SqsClient.class);
    }

    @Bean
    EventMessageConsumer eventMessageConsumer() {
      return mock(EventMessageConsumer.class);
    }
  }

  @Test
  void startsPollingWhenQueueNamePrefixIsConfigured() {
    contextRunner
        .run(context -> assertThat(context).hasSingleBean(SmartLifecycle.class));
  }

  @Test
  void failsWhenNoSqsClientIsProvided() {
    new ApplicationContextRunner()
        .withConfiguration(autoConfigurations)
        .withUserConfiguration(ApplicationConfigurationWithoutSqsClient.class)
        .withPropertyValues("caravan.event.messaging.queue-name-prefix=test-app")
        .run(context -> assertThat(context).hasFailed());
  }

  @Test
  void shouldQueryQueueUrlForSubscribedQueueName() {
    contextRunner.run(context -> {
      var sqsClient = context.getBean(SqsClient.class);
      verify(sqsClient).getQueueUrl(
          GetQueueUrlRequest.builder().queueName("test-app_calculator").build());
    });
  }

  @Test
  void shouldQueriesWithCorrectQueueUrl() {
    contextRunner.run(context -> {
      var sqsClient = context.getBean(SqsClient.class);
      var lifecycle = context.getBean(SmartLifecycle.class);

      try {
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
            verify(sqsClient, atLeastOnce()).receiveMessage(
                argThat((ReceiveMessageRequest request) -> QUEUE_URL.equals(request.queueUrl()))));
      } finally {
        lifecycle.stop();
      }
    });
  }

  @Test
  void shouldFailWhenGracefulShutdownSecondsValueIsNegative() {
    contextRunner
        .withPropertyValues("caravan.event.messaging.graceful-shutdown-seconds=-1")
        .run(context -> assertThat(context).hasFailed());
  }

  @Test
  void startsNoPollersWhenNoQueuesAreSubscribed() {
    new ApplicationContextRunner()
        .withConfiguration(autoConfigurations)
        .withUserConfiguration(ApplicationConfigurationWithNoSubscribedQueues.class)
        .withPropertyValues("caravan.event.messaging.queue-name-prefix=test-app")
        .run(context -> {
          var sqsClient = context.getBean(SqsClient.class);
          var lifecycle = context.getBean(SmartLifecycle.class);

          assertThat(lifecycle.isRunning()).isFalse();
          verify(sqsClient, never()).getQueueUrl(any(GetQueueUrlRequest.class));
        });
  }

  @Test
  void failsWhenQueueNamePrefixIsNotProvided() {
    new ApplicationContextRunner()
        .withConfiguration(autoConfigurations)
        .withUserConfiguration(ApplicationConfiguration.class)
        .run(context -> assertThat(context).hasFailed());
  }

  @Nested
  class MessagingProperties {

    @Test
    void appliesDefaultsWhenNothingIsConfigured() {
      contextRunner.run(context -> {
        assertThat(context).getBean(QueuePollingProperties.class)
            .satisfies(properties -> {
              assertThat(properties.concurrency()).isEqualTo(10);
              assertThat(properties.maxPollSize()).isEqualTo(10);
              assertThat(properties.minPollSize()).isEqualTo(3);
              assertThat(properties.pollersCountCap()).isEqualTo(0);
              assertThat(properties.pollWaitSeconds()).isEqualTo(10);
              assertThat(properties.messageBatchDeletionProperties().maxBatchSize())
                  .isEqualTo(10);
              assertThat(properties.messageBatchDeletionProperties().periodSeconds())
                  .isEqualTo(1);
              assertThat(properties.messageBatchDeletionProperties().concurrency())
                  .isEqualTo(3);
            });
        assertThat(context).getBean(CaravanQueuePollingConfigurationProperties.class)
            .satisfies(properties -> assertThat(properties.gracefulShutdownSeconds()).isEqualTo(10));
      });
    }

    @Test
    void bindsConfiguredValues() {
      contextRunner
          .withPropertyValues(
              "caravan.event.messaging.concurrency=25",
              "caravan.event.messaging.max-poll-size=15",
              "caravan.event.messaging.min-poll-size=2",
              "caravan.event.messaging.pollers-count-cap=12",
              "caravan.event.messaging.poll-wait-seconds=7",
              "caravan.event.messaging.graceful-shutdown-seconds=17",
              "caravan.event.messaging.deletion.max-batch-size=7",
              "caravan.event.messaging.deletion.period-seconds=9",
              "caravan.event.messaging.deletion.concurrency=2"
          )
          .run(context -> {
            assertThat(context).getBean(QueuePollingProperties.class)
                .satisfies(properties -> {
                  assertThat(properties.concurrency()).isEqualTo(25);
                  assertThat(properties.maxPollSize()).isEqualTo(15);
                  assertThat(properties.minPollSize()).isEqualTo(2);
                  assertThat(properties.pollersCountCap()).isEqualTo(12);
                  assertThat(properties.pollWaitSeconds()).isEqualTo(7);
                  assertThat(properties.messageBatchDeletionProperties().maxBatchSize())
                      .isEqualTo(7);
                  assertThat(properties.messageBatchDeletionProperties().periodSeconds())
                      .isEqualTo(9);
                  assertThat(properties.messageBatchDeletionProperties().concurrency())
                      .isEqualTo(2);
                });
            assertThat(context).getBean(CaravanQueuePollingConfigurationProperties.class)
                .satisfies(properties -> assertThat(properties.gracefulShutdownSeconds()).isEqualTo(17));
          });
    }

    @Test
    void applicationCanSupplyOwnQueuePollingProperties() {
      var ownQueuePollingProperties = QueuePollingProperties.builder()
          .concurrency(1)
          .maxPollSize(1)
          .minPollSize(1)
          .pollersCountCap(0)
          .pollWaitSeconds(1)
          .messageBatchDeletionProperties(
              MessageBatchDeletionProperties.builder()
                  .maxBatchSize(1)
                  .periodSeconds(1)
                  .concurrency(1)
                  .build())
          .build();

      contextRunner
          .withBean(QueuePollingProperties.class, () -> ownQueuePollingProperties)
          .run(context ->
              assertThat(context).getBean(QueuePollingProperties.class).isSameAs(ownQueuePollingProperties));
    }
  }
}
