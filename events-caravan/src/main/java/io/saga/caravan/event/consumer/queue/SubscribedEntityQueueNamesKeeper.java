package io.saga.caravan.event.consumer.queue;

import io.saga.caravan.event.EntityEventsRegistration;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.saga.caravan.utils.TextUtils.hasText;
import static java.util.stream.Collectors.toSet;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class SubscribedEntityQueueNamesKeeper {

  private static final String QUEUE_NAME_TEMPLATE = "%s_%s";

  public static SubscribedEntityQueueNamesKeeper create(Collection<EntityEventsRegistration> entityEventsRegistrations,
                                                        String queueNamePrefix) {
    if (!hasText(queueNamePrefix)) {
      throw new QueuesSetupException("Queue name prefix must be present");
    }

    Map<String, String> resultMap = new HashMap<>();

    var subscribedEntityNames = subscribedEntityNames(entityEventsRegistrations);

    entityEventsRegistrations.stream()
        .map(EntityEventsRegistration::entityName)
        .filter(subscribedEntityNames::contains)
        .forEach(entityName -> {
          if (resultMap.containsKey(entityName)) {
            throw new QueuesSetupException("entityName=%s is duplicated".formatted(entityName));
          }

          resultMap.put(
              entityName,
              QUEUE_NAME_TEMPLATE.formatted(queueNamePrefix, entityName));
        });

    return new SubscribedEntityQueueNamesKeeper(resultMap);
  }

  private static Set<String> subscribedEntityNames(
      Collection<EntityEventsRegistration> entityEventsRegistrations) {

    return entityEventsRegistrations.stream()
        .filter(EntityEventsRegistration::isSubscriptionActive)
        .map(EntityEventsRegistration::entityName)
        .collect(toSet());
  }

  private final Map<String, String> byEntityName;

  public Optional<String> queueNameOf(String entityName) {
    return Optional.ofNullable(byEntityName.get(entityName));
  }

  public Collection<String> queueNames() {
    return byEntityName.values().stream()
        .distinct()
        .toList();
  }
}
