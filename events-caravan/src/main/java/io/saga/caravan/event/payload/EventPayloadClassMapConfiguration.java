package io.saga.caravan.event.payload;

import io.saga.caravan.event.EntityEventsRegistration;
import io.saga.caravan.event.EventType;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class EventPayloadClassMapConfiguration {

  @Bean(name = "eventPayloadClassMap")
  public Map<EventType, Class<?>> eventPayloadClassMap(
      Collection<EntityEventsRegistration> entityEventsRegistrations) {

    Map<EventType, Class<?>> result = new HashMap<>();

    entityEventsRegistrations.forEach(entityEventsRegistration -> {
      var entityName = entityEventsRegistration.entityName();
      entityEventsRegistration.eventToPayloadClass()
          .forEach((eventName, eventPayloadClass) -> {
            validate(eventPayloadClass);
            register(result, new EventType(entityName, eventName), eventPayloadClass);
          });
    });

    return result;
  }

  private void register(Map<EventType, Class<?>> eventPayloadClassMap,
                        EventType eventType,
                        Class<?> eventPayloadClass) {
    var alreadyRegisteredClass = eventPayloadClassMap.putIfAbsent(eventType, eventPayloadClass);
    if (alreadyRegisteredClass != null) {
      throw new EventPayloadRegistrationException(
          "Event payload class must be registered exactly once, which is not the case for %s: registered both %s and %s"
              .formatted(eventType, alreadyRegisteredClass, eventPayloadClass));
    }
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
