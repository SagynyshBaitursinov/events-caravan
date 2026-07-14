package io.saga.caravan.event.sourcing.applying;

import io.saga.caravan.event.EventType;
import io.saga.caravan.event.sourcing.EntityEventApplier;
import io.saga.caravan.event.sourcing.EventSourcedEntity;
import io.saga.caravan.event.sourcing.EventSourcedEntityNamesKeeper;
import io.saga.caravan.event.sourcing.EventSourcedEntitySetupException;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

@Component
@RequiredArgsConstructor
public class ApplyEventMethodsValidator implements SmartInitializingSingleton {

  private final Map<EventType, Class<?>> eventPayloadClassMap;
  private final EventSourcedEntityNamesKeeper eventSourcedEntityNamesKeeper;

  @Override
  public void afterSingletonsInstantiated() {
    eventSourcedEntityNamesKeeper.getEntityClassToEntityNameMap()
        .forEach(this::validateEntity);
  }

  private void validateEntity(Class<? extends EventSourcedEntity> entityClass,
                              String entityName) {
    Map<String, Method> applyEventMethods = EntityEventApplier.applyEventMethodsOf(entityClass);

    applyEventMethods.forEach((eventName, method) ->
        validatePayloadClass(entityName, eventName, method));

    validateEventApplierCoverage(entityName, applyEventMethods.keySet());
  }

  private void validatePayloadClass(String entityName,
                                    String eventName,
                                    Method applyEventMethod) {
    var registeredPayloadClass
        = eventPayloadClassMap.get(new EventType(entityName, eventName));

    var parametrizedType = (ParameterizedType) applyEventMethod.getGenericParameterTypes()[0];
    if (!parametrizedType.getActualTypeArguments()[0].equals(registeredPayloadClass)) {
      throw new EventSourcedEntitySetupException(
          "@ApplyEvent Event parameter's payload class must be the one from EventPayloadRegistration, which is not the case for %s.%s"
              .formatted(
                  applyEventMethod.getDeclaringClass().getName(),
                  applyEventMethod.getName()));
    }
  }

  private void validateEventApplierCoverage(String entityName,
                                            Set<String> coveredEventNames) {
    Set<EventType> missingApplyEventMethod = eventPayloadClassMap.keySet().stream()
        .filter(eventType -> eventType.entityName().equals(entityName))
        .filter(eventType -> !coveredEventNames.contains(eventType.eventName()))
        .collect(toSet());

    if (!missingApplyEventMethod.isEmpty()) {
      throw new EventSourcedEntitySetupException(
          "Events with no @ApplyEvent method: %s".formatted(missingApplyEventMethod));
    }
  }
}
