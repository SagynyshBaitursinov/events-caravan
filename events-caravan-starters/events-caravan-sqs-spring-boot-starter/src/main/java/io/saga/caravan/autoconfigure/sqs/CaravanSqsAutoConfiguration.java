package io.saga.caravan.autoconfigure.sqs;

import io.saga.caravan.autoconfigure.CaravanEventDrivenComponentsAutoConfiguration;
import io.saga.caravan.event.EntityEventsRegistration;
import io.saga.caravan.event.consumer.EventMessageConsumer;
import io.saga.caravan.queue.polling.ContinuousMessagePollingController;
import io.saga.caravan.queue.polling.QueuePollingProperties;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.time.Instant;
import java.util.Collection;

import static io.saga.caravan.event.consumer.queue.sqs.SqsUtils.deleteMessages;
import static io.saga.caravan.event.consumer.queue.sqs.SqsUtils.getQueueUrl;
import static io.saga.caravan.event.consumer.queue.sqs.SqsUtils.pollMessagesFromQueue;

@AutoConfiguration(after = CaravanEventDrivenComponentsAutoConfiguration.class)
@EnableConfigurationProperties(CaravanQueuePollingConfigurationProperties.class)
public class CaravanSqsAutoConfiguration {

  private static final String QUEUE_NAME_TEMPLATE = "%s_%s";

  @Bean
  @ConditionalOnMissingBean
  public QueuePollingProperties queuePollingProperties(
      CaravanQueuePollingConfigurationProperties properties) {

    return properties.toMessagingProperties();
  }

  @Bean
  public SmartLifecycle sqsMessagePollingControllerLifecycle(
      SqsClient sqsClient,
      EventMessageConsumer eventMessageConsumer,
      ObjectProvider<EntityEventsRegistration> entityEventsRegistrations,
      QueuePollingProperties queuePollingProperties,
      CaravanQueuePollingConfigurationProperties caravanQueuePollingConfigurationProperties) {

    String queueNamePrefix = caravanQueuePollingConfigurationProperties.queueNamePrefix();
    if (queueNamePrefix == null || queueNamePrefix.isBlank()) {
      throw new SqsQueuesSetupException("Queue name prefix must be present");
    }

    var sqsPollingControllers = subscribedQueueNames(entityEventsRegistrations, queueNamePrefix)
        .stream()
        .map(queueName ->
            createSqsPollingController(
                sqsClient,
                eventMessageConsumer,
                queuePollingProperties,
                queueName,
                getQueueUrl(sqsClient, queueName)))
        .toList();

    int gracefulShutdownSeconds = caravanQueuePollingConfigurationProperties.gracefulShutdownSeconds();
    if (gracefulShutdownSeconds < 0) {
      throw new SqsQueuesSetupException("gracefulShutdownSeconds must be greater or equal to 0");
    }

    return new SmartLifecycle() {

      @Override
      public void start() {
        sqsPollingControllers.forEach(ContinuousMessagePollingController::startContinuousPolling);
      }

      @Override
      public void stop() {
        var deadline = Instant.now().plusSeconds(gracefulShutdownSeconds);
        sqsPollingControllers.forEach(ContinuousMessagePollingController::requestStopOfContinuousPolling);
        sqsPollingControllers.forEach(pollingController -> pollingController.awaitStopOfContinuousPolling(deadline));
      }

      @Override
      public boolean isRunning() {
        return sqsPollingControllers.stream().anyMatch(ContinuousMessagePollingController::isContinuousPollingRunning);
      }
    };
  }

  private Collection<String> subscribedQueueNames(ObjectProvider<EntityEventsRegistration> entityEventsRegistrations,
                                                  String queueNamePrefix) {
    return entityEventsRegistrations.stream()
        .filter(EntityEventsRegistration::isSubscriptionActive)
        .map(EntityEventsRegistration::entityName)
        .distinct()
        .map(entityName -> toQueueName(queueNamePrefix, entityName))
        .toList();
  }

  private static String toQueueName(String queueNamePrefix, String entityName) {
    return QUEUE_NAME_TEMPLATE.formatted(queueNamePrefix, entityName);
  }

  private ContinuousMessagePollingController createSqsPollingController(SqsClient sqsClient,
                                                                        EventMessageConsumer eventMessageConsumer,
                                                                        QueuePollingProperties queuePollingProperties,
                                                                        String queueName,
                                                                        String sqsQueueUrl) {
    return ContinuousMessagePollingController.builder()
        .queuePollingProperties(queuePollingProperties)
        .queueName(queueName)
        .messagesPoller((pollingRequest) -> pollMessagesFromQueue(sqsClient, sqsQueueUrl, pollingRequest))
        .messageConsumer(message -> eventMessageConsumer.consume(message.body()))
        .messagesDeleter(messages -> deleteMessages(sqsClient, sqsQueueUrl, messages))
        .build();
  }
}
