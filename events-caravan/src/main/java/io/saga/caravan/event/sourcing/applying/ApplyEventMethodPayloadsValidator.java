package io.saga.caravan.event.sourcing.applying;

import io.saga.caravan.event.EventPayloadClassMappingKeeper;
import io.saga.caravan.event.EventType;
import io.saga.caravan.event.sourcing.EventSourcedEntity;
import io.saga.caravan.event.sourcing.EventSourcedEntitySetupException;
import lombok.RequiredArgsConstructor;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

@RequiredArgsConstructor
public class ApplyEventMethodPayloadsValidator {

  private final EventPayloadClassMappingKeeper eventPayloadClassMappingKeeper;

  public void validate(String entityName,
                       Class<? extends EventSourcedEntity> entityClass) {
    Map<String, Method> applyEventMethods
        = ApplyMethodsCollector.applyEventMethodsOf(entityClass);

    applyEventMethods.forEach((eventName, method) ->
        validatePayloadClass(entityName, eventName, method));

    validateEventApplierCoverage(entityName, applyEventMethods.keySet());
  }

  private void validatePayloadClass(String entityName,
                                    String eventName,
                                    Method applyEventMethod) {
    var eventType = new EventType(entityName, eventName);
    var registeredEventPayloadClass = eventPayloadClassMappingKeeper.payloadClassFor(eventType)
        .orElseThrow(() -> new EventSourcedEntitySetupException(
            "There's no registered event payload class for %s".formatted(eventType)));

    var parameterTypes = applyEventMethod.getGenericParameterTypes();
    var parametrizedType = (ParameterizedType) parameterTypes[parameterTypes.length - 1];
    if (!parametrizedType.getActualTypeArguments()[0].equals(registeredEventPayloadClass)) {
      throw new EventSourcedEntitySetupException(
          "@ApplyEvent Event parameter's payload class must be the one from EventPayloadRegistration, which is not the case for %s.%s"
              .formatted(
                  applyEventMethod.getDeclaringClass().getName(),
                  applyEventMethod.getName()));
    }
  }

  private void validateEventApplierCoverage(String entityName,
                                            Set<String> coveredEventNames) {
    Set<EventType> missingApplyEventMethod = eventPayloadClassMappingKeeper.registeredEventTypes().stream()
        .filter(eventType -> eventType.entityName().equals(entityName))
        .filter(eventType -> !coveredEventNames.contains(eventType.eventName()))
        .collect(toSet());

    if (!missingApplyEventMethod.isEmpty()) {
      throw new EventSourcedEntitySetupException(
          "Events with no @ApplyEvent method: %s".formatted(missingApplyEventMethod));
    }
  }
}
