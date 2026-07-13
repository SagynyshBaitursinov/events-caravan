package io.saga.caravan.event.sourcing.applying;

import io.saga.caravan.event.EventType;
import io.saga.caravan.event.sourcing.EntityEventApplier;
import io.saga.caravan.event.sourcing.EventSourcedEntityNamesKeeper;
import io.saga.caravan.event.sourcing.EventSourcedEntitySetupException;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

@Component
@RequiredArgsConstructor
public class EntityEventApplierConfiguration {

  private final Map<EventType, Method> applyEventMethodsMap = new HashMap<>();
  private final Map<EventType, Class<?>> eventPayloadClassMap;
  private final EventSourcedEntityNamesKeeper eventSourcedEntityNamesKeeper;
  private final ApplyMethodsCollector applyMethodsCollector;

  @Bean
  public EntityEventApplier entityEventApplier() {
    return new EntityEventApplier(applyEventMethodsMap);
  }

  @Bean
  public SmartInitializingSingleton eventTypeApplyMethodMap(EventSourcedEntityNamesKeeper eventSourcedEntityNamesKeeper) {
    return () -> {
      eventSourcedEntityNamesKeeper.getEntityClassToEntityNameMap()
          .forEach((entityClass, entityName) ->
              applyEventMethodsMap.putAll(
                  applyMethodsCollector.collectApplyEventMethods(entityClass, entityName)));

      validateEventApplierCoverage();
    };
  }

  private void validateEventApplierCoverage() {
    Set<EventType> missingApplyEventMethod = eventPayloadClassMap.keySet().stream()
        .filter(this::belongsToEventSourcedEntity)
        .filter(this::isNotInApplyEventMethodsMap)
        .collect(toSet());

    if (!missingApplyEventMethod.isEmpty()) {
      throw new EventSourcedEntitySetupException(
          "Events with no @ApplyEvent method: %s".formatted(missingApplyEventMethod));
    }
  }

  private boolean belongsToEventSourcedEntity(EventType eventType) {
    return eventSourcedEntityNamesKeeper.getEntityNames()
        .contains(eventType.entityName());
  }

  private boolean isNotInApplyEventMethodsMap(EventType eventType) {
    return !applyEventMethodsMap.containsKey(eventType);
  }
}

