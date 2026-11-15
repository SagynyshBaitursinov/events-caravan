package dev.baitursinov.caravan.event.publisher.sns;

import dev.baitursinov.caravan.event.EventReference;
import dev.baitursinov.caravan.event.publisher.EventMessage;
import dev.baitursinov.caravan.event.publisher.EventMessagePublisher;
import dev.baitursinov.caravan.event.publisher.EventPublicationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.BatchRequestTooLongException;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishBatchRequest;
import software.amazon.awssdk.services.sns.model.PublishBatchRequestEntry;
import software.amazon.awssdk.services.sns.model.PublishRequest;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

/**
 * {@link EventMessagePublisher} publishing event messages into SNS, one topic per
 * {@code entityName} resolved by {@link SnsTopicArnResolver}.
 *
 * <p>Messages are grouped by topic into {@code PublishBatch} requests of up to 10 entries, sent
 * sequentially. A batch rejected as too large is retried entry by entry. Standard topics give
 * at-least-once, unordered delivery.
 */
@Slf4j
@RequiredArgsConstructor
public class SnsEventMessagePublisher implements EventMessagePublisher {

  static final String ENTITY_NAME_ATTRIBUTE = "entityName";
  static final String EVENT_NAME_ATTRIBUTE = "eventName";

  private static final int MAX_ENTRIES_PER_BATCH = 10;

  private final SnsClient snsClient;
  private final SnsTopicArnResolver topicArnResolver;

  @Override
  public void publish(EventMessage eventMessage) {
    publish(List.of(eventMessage));
  }

  @Override
  public void publish(List<EventMessage> eventMessages) {
    List<EventReference> failed = new ArrayList<>();
    Exception lastFailure = null;

    for (var topicMessages : groupByEntityName(eventMessages).entrySet()) {
      String topicArn;
      try {
        topicArn = topicArnResolver.topicArnFor(topicMessages.getKey());
      } catch (RuntimeException exception) {
        log.warn("Could not resolve topic for entityName={}", topicMessages.getKey(), exception);
        topicMessages.getValue().forEach(message -> failed.add(message.eventReference()));
        lastFailure = exception;
        continue;
      }

      var messages = topicMessages.getValue();
      for (int from = 0; from < messages.size(); from += MAX_ENTRIES_PER_BATCH) {
        var batch = messages.subList(from, Math.min(from + MAX_ENTRIES_PER_BATCH, messages.size()));
        try {
          failed.addAll(publishBatch(topicArn, batch));
        } catch (RuntimeException exception) {
          log.warn("Could not publish a batch of {} messages to {}", batch.size(), topicArn, exception);
          batch.forEach(message -> failed.add(message.eventReference()));
          lastFailure = exception;
        }
      }
    }

    if (!failed.isEmpty()) {
      var message = "Could not publish %d of %d event messages to SNS: %s"
          .formatted(failed.size(), eventMessages.size(), failed);
      throw lastFailure == null
          ? new EventPublicationException(failed, message)
          : new EventPublicationException(failed, message, lastFailure);
    }
  }

  private List<EventReference> publishBatch(String topicArn, List<EventMessage> batch) {
    if (batch.size() == 1) {
      publishOne(topicArn, batch.getFirst());
      return List.of();
    }

    var entries = IntStream.range(0, batch.size())
        .mapToObj(index -> PublishBatchRequestEntry.builder()
            .id(String.valueOf(index))
            .message(batch.get(index).body())
            .messageAttributes(attributes(batch.get(index).eventReference()))
            .build())
        .toList();

    try {
      var response = snsClient.publishBatch(
          PublishBatchRequest.builder()
              .topicArn(topicArn)
              .publishBatchRequestEntries(entries)
              .build());

      return response.failed().stream()
          .map(failure -> {
            var reference = batch.get(Integer.parseInt(failure.id())).eventReference();
            log.warn("Failed to publish {} to {}: code={}, message={}",
                reference, topicArn, failure.code(), failure.message());
            return reference;
          })
          .toList();
    } catch (BatchRequestTooLongException exception) {
      log.debug("Batch to {} is too large, publishing its {} messages one by one", topicArn, batch.size());
      return publishOneByOne(topicArn, batch);
    }
  }

  private List<EventReference> publishOneByOne(String topicArn, List<EventMessage> batch) {
    List<EventReference> failed = new ArrayList<>();
    for (var message : batch) {
      try {
        publishOne(topicArn, message);
      } catch (RuntimeException exception) {
        log.warn("Failed to publish {} to {}", message.eventReference(), topicArn, exception);
        failed.add(message.eventReference());
      }
    }
    return failed;
  }

  private void publishOne(String topicArn, EventMessage message) {
    snsClient.publish(
        PublishRequest.builder()
            .topicArn(topicArn)
            .message(message.body())
            .messageAttributes(attributes(message.eventReference()))
            .build());
  }

  private static Map<String, List<EventMessage>> groupByEntityName(List<EventMessage> eventMessages) {
    return eventMessages.stream()
        .collect(groupingBy(
            message -> message.eventReference().entityReference().entityName(),
            LinkedHashMap::new,
            toList()));
  }

  private static Map<String, MessageAttributeValue> attributes(EventReference eventReference) {
    return Map.of(
        ENTITY_NAME_ATTRIBUTE, stringAttribute(eventReference.entityReference().entityName()),
        EVENT_NAME_ATTRIBUTE, stringAttribute(eventReference.eventName()));
  }

  private static MessageAttributeValue stringAttribute(String value) {
    return MessageAttributeValue.builder()
        .dataType("String")
        .stringValue(value)
        .build();
  }
}
