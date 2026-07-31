package io.saga.caravan.autoconfigure.sqs;

import io.saga.caravan.autoconfigure.CaravanEventMessagingAutoConfiguration;
import io.saga.caravan.event.consumer.EventMessageConsumer;
import io.saga.caravan.event.consumer.queue.SubscribedEntityQueueNamesKeeper;
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
import java.util.List;

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

  AutoConfigurations autoConfigurations = AutoConfigurations.of(
      CaravanEventMessagingAutoConfiguration.class,
      CaravanSqsAutoConfiguration.class);

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
    SubscribedEntityQueueNamesKeeper subscribedEntityQueueNamesKeeper() {
      SubscribedEntityQueueNamesKeeper mock = mock(SubscribedEntityQueueNamesKeeper.class);
      when(mock.queueNames()).thenReturn(List.of("test-app_calculator"));
      return mock;
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class ApplicationConfigurationWithoutSqsClient {

    @Bean
    EventMessageConsumer eventMessageConsumer() {
      return mock(EventMessageConsumer.class);
    }

    @Bean
    SubscribedEntityQueueNamesKeeper subscribedEntityQueueNamesKeeper() {
      SubscribedEntityQueueNamesKeeper mock = mock(SubscribedEntityQueueNamesKeeper.class);
      when(mock.queueNames()).thenReturn(List.of("test-app_calculator"));
      return mock;
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

    @Bean
    SubscribedEntityQueueNamesKeeper subscribedEntityQueueNamesKeeper() {
      SubscribedEntityQueueNamesKeeper mock = mock(SubscribedEntityQueueNamesKeeper.class);
      when(mock.queueNames()).thenReturn(List.of());
      return mock;
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
}
