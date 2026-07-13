package io.saga.caravan.event.payload;

import io.saga.caravan.event.EventType;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

@Configuration
public class EventPayloadClassMapConfiguration {

  @Bean(name = "eventPayloadClassMap")
  public Map<EventType, Class<?>> eventPayloadClassMap(
      Collection<EventPayloadRegistration> eventPayloadRegistrations) {

    return eventPayloadRegistrations.stream()
        .flatMap(entityEventsRegistration -> {
          var entityName = entityEventsRegistration.entityName();
          return entityEventsRegistration.eventToPayloadClass().entrySet().stream()
              .map(entry -> {
                var eventName = entry.getKey();
                var eventPayloadClass = entry.getValue();
                validate(eventPayloadClass);
                return Map.entry(
                    new EventType(entityName, eventName),
                    eventPayloadClass);
              });
        })
        .collect(toMap(
            Map.Entry::getKey,
            Map.Entry::getValue));
  }

  private void validate(Class<?> eventPayloadClass) {
    if (isAbstract(eventPayloadClass)
        || isInterface(eventPayloadClass)
        || isArray(eventPayloadClass)
        || isPrimitive(eventPayloadClass)
        || isInnerNonStaticClass(eventPayloadClass)
        || isAnonymousClass(eventPayloadClass)
        || isLocalClass(eventPayloadClass)
        || isEnum(eventPayloadClass)
        || isJavaBuiltIn(eventPayloadClass)) {
      throw new EventPayloadRegistrationException(
          "Event payload class must be concrete class, which is not the case for %s"
              .formatted(eventPayloadClass));
    }
  }

  private boolean isAbstract(Class<?> clazz) {
    return Modifier.isAbstract(clazz.getModifiers());
  }

  private boolean isInterface(Class<?> clazz) {
    return clazz.isInterface();
  }

  private boolean isArray(Class<?> clazz) {
    return clazz.isArray();
  }

  private boolean isPrimitive(Class<?> clazz) {
    return clazz.isPrimitive();
  }

  private boolean isInnerNonStaticClass(Class<?> clazz) {
    return clazz.isMemberClass() && !Modifier.isStatic(clazz.getModifiers());
  }

  private boolean isAnonymousClass(Class<?> clazz) {
    return clazz.isAnonymousClass();
  }

  private boolean isLocalClass(Class<?> clazz) {
    return clazz.isLocalClass();
  }

  private boolean isEnum(Class<?> clazz) {
    return clazz.isEnum();
  }

  private boolean isJavaBuiltIn(Class<?> clazz) {
    String name = clazz.getName();
    return name.startsWith("java.")
        || name.startsWith("javax.")
        || name.startsWith("sun.")
        || name.startsWith("com.sun.");
  }
}
