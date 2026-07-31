package io.saga.caravan.autoconfigure.sqs;

import io.saga.caravan.autoconfigure.CaravanEventMessagingAutoConfiguration;
import io.saga.caravan.autoconfigure.CaravanMessagingConfigurationProperties;
import io.saga.caravan.event.consumer.EventMessageConsumer;
import io.saga.caravan.event.consumer.queue.SubscribedEntityQueueNamesKeeper;
import io.saga.caravan.messaging.ContinuousPollingMessageProcessor;
import io.saga.caravan.messaging.MessagingProperties;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.time.Instant;

import static io.saga.caravan.event.consumer.queue.sqs.SqsUtils.deleteMessages;
import static io.saga.caravan.event.consumer.queue.sqs.SqsUtils.getQueueUrl;
import static io.saga.caravan.event.consumer.queue.sqs.SqsUtils.pollMessagesFromQueue;

@AutoConfiguration(after = CaravanEventMessagingAutoConfiguration.class)
public class CaravanSqsAutoConfiguration {

  @Bean
  public SmartLifecycle sqsMessagePollersLifecycle(SqsClient sqsClient,
                                                   EventMessageConsumer eventMessageConsumer,
                                                   SubscribedEntityQueueNamesKeeper subscribedEntityQueueNamesKeeper,
                                                   CaravanMessagingConfigurationProperties caravanMessagingConfigurationProperties) {
    var messagingProperties = caravanMessagingConfigurationProperties.toMessagingProperties();
    var sqsPollers = subscribedEntityQueueNamesKeeper.queueNames().stream()
        .map(queueName ->
            createSqsQueueMessagePoller(
                sqsClient,
                eventMessageConsumer,
                messagingProperties,
                queueName,
                getQueueUrl(sqsClient, queueName)))
        .toList();
    int gracefulShutdownSeconds = caravanMessagingConfigurationProperties.gracefulShutdownSeconds();
    if (gracefulShutdownSeconds < 0) {
      throw new IllegalArgumentException("gracefulShutdownSeconds must be greater or equal to 0");
    }

    return new SmartLifecycle() {

      @Override
      public void start() {
        sqsPollers.forEach(ContinuousPollingMessageProcessor::startContinuousPolling);
      }

      @Override
      public void stop() {
        var deadline = Instant.now().plusSeconds(gracefulShutdownSeconds);
        sqsPollers.forEach(ContinuousPollingMessageProcessor::requestStopOfContinuousPolling);
        sqsPollers.forEach(poller -> poller.awaitStopOfContinuousPolling(deadline));
      }

      @Override
      public boolean isRunning() {
        return sqsPollers.stream().anyMatch(ContinuousPollingMessageProcessor::isContinuousPollingRunning);
      }
    };
  }

  private ContinuousPollingMessageProcessor createSqsQueueMessagePoller(
      SqsClient sqsClient,
      EventMessageConsumer eventMessageConsumer,
      MessagingProperties messagingProperties,
      String queueName,
      String sqsQueueUrl) {

    return ContinuousPollingMessageProcessor.builder()
        .messagingProperties(messagingProperties)
        .queueName(queueName)
        .messagesPoller((pollingRequest) -> pollMessagesFromQueue(sqsClient, sqsQueueUrl, pollingRequest))
        .messageConsumer(message -> eventMessageConsumer.consume(message.body()))
        .messagesDeleter(messages -> deleteMessages(sqsClient, sqsQueueUrl, messages))
        .build();
  }
}
