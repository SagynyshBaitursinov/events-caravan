package io.saga.caravan.event.consumer.messaging.sqs;

import io.saga.caravan.event.consumer.messaging.EventMessageConsumer;
import io.saga.caravan.messaging.ContinuousPollingMessageProcessor;
import io.saga.caravan.messaging.MessagingProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.VirtualThreadTaskExecutor;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static io.saga.caravan.event.consumer.messaging.sqs.SqsUtils.deleteMessage;
import static io.saga.caravan.event.consumer.messaging.sqs.SqsUtils.getQueueUrl;
import static io.saga.caravan.event.consumer.messaging.sqs.SqsUtils.pollMessagesFromQueue;

@Slf4j
@Configuration
public class SqsMessagePollersConfiguration {

  private final SqsClient sqsClient;
  private final EventMessageConsumer eventMessageConsumer;
  private final Map<String, String> entityNameToQueueName;
  private final MessagingProperties messagingProperties;
  private final AsyncTaskExecutor messageHandlingTaskExecutor;

  public SqsMessagePollersConfiguration(SqsClient sqsClient,
                                        EventMessageConsumer eventMessageConsumer,
                                        @Qualifier("entityNameToQueueName") Map<String, String> entityNameToQueueName,
                                        MessagingProperties messagingProperties) {
    this.sqsClient = sqsClient;
    this.eventMessageConsumer = eventMessageConsumer;
    this.entityNameToQueueName = entityNameToQueueName;
    this.messagingProperties = messagingProperties;
    this.messageHandlingTaskExecutor = new VirtualThreadTaskExecutor("sqs-process-");
  }

  @Bean
  public SmartLifecycle sqsMessagePollersLifecycle() {
    var sqsMessagePollers = createSqsMessagePollersForEveryQueue();

    return new SmartLifecycle() {

      @Override
      public void start() {
        log.info("Starting {} SqsMessagePollers", sqsMessagePollers.size());
        sqsMessagePollers.forEach(ContinuousPollingMessageProcessor::startContinuousPolling);
        log.info("All SqsMessagePollers are started");
      }

      @Override
      public void stop() {
        log.info("Stopping {} SqsMessagePollers", sqsMessagePollers.size());
        var deadline = Instant.now().plusSeconds(messagingProperties.gracefulShutdownSeconds());
        sqsMessagePollers.forEach(ContinuousPollingMessageProcessor::requestStopOfContinuousPolling);
        sqsMessagePollers.forEach(poller -> poller.awaitStopOfContinuousPolling(deadline));
        log.info("All SqsMessagePollers are stopped");
      }

      @Override
      public boolean isRunning() {
        return sqsMessagePollers.stream()
            .anyMatch(ContinuousPollingMessageProcessor::isContinuousPollingRunning);
      }
    };
  }

  private List<ContinuousPollingMessageProcessor> createSqsMessagePollersForEveryQueue() {
    return entityNameToQueueName.values().stream()
        .distinct()
        .map(queueName ->
            createSqsQueueMessagePoller(
                queueName,
                getQueueUrl(sqsClient, queueName)))
        .toList();
  }

  private ContinuousPollingMessageProcessor createSqsQueueMessagePoller(String queueName,
                                                                        String sqsQueueUrl) {
    return ContinuousPollingMessageProcessor.builder()
        .messageHandlingExecutor(messageHandlingTaskExecutor)
        .messagingProperties(messagingProperties)
        .queueName(queueName)
        .pollMessages((pollingRequest) -> pollMessagesFromQueue(sqsClient, sqsQueueUrl, pollingRequest))
        .consumeMessage(eventMessageConsumer)
        .deleteMessage(message -> deleteMessage(sqsClient, sqsQueueUrl, message))
        .build();
  }
}
