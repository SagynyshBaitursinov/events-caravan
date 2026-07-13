package io.saga.caravan.event.consumer.messaging;

import io.saga.caravan.event.payload.EventPayloadRegistration;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class EventMessageQueueMappingConfiguration {

  private static final String QUEUE_NAME_TEMPLATE = "%s_%s";

  private final Collection<EventPayloadRegistration> eventPayloadRegistrations;

  @Bean(name = "entityNameToQueueName")
  public Map<String, String> entityNameToQueueName(
      @Value("${caravan.event.message.queue.name-prefix}") String eventMessageQueueNamePrefix) {

    Map<String, String> result = new HashMap<>();

    var subscribedEntityNames = subscribedEntityNames();

    eventPayloadRegistrations.stream()
        .map(EventPayloadRegistration::entityName)
        .filter(subscribedEntityNames::contains)
        .forEach(entityName ->
            result.computeIfAbsent(
                entityName,
                entityNameAsKey ->
                    QUEUE_NAME_TEMPLATE.formatted(
                        eventMessageQueueNamePrefix, entityNameAsKey)));

    return result;
  }

  private Set<String> subscribedEntityNames() {
    return eventPayloadRegistrations.stream()
        .filter(EventPayloadRegistration::isIncomingSubscriptionActive)
        .map(EventPayloadRegistration::entityName)
        .collect(toSet());
  }
}
