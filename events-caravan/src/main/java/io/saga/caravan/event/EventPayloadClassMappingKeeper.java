package io.saga.caravan.event;

import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class EventPayloadClassMappingKeeper {

  private final Map<EventType, Class<?>> map = new HashMap<>();

  public static EventPayloadClassMappingKeeper create(
      Collection<EntityEventsRegistration> entityEventsRegistrations) {

    var result = new EventPayloadClassMappingKeeper();

    entityEventsRegistrations.forEach(
        entityEventsRegistration -> {
          var entityName = entityEventsRegistration.entityName();

          entityEventsRegistration.eventToPayloadClass()
              .forEach((eventName, eventPayloadClass) -> {
                validate(eventPayloadClass);
                result.register(
                    new EventType(entityName, eventName),
                    eventPayloadClass);
              });
        });

    return result;
  }

  public EventPayloadClassMappingKeeper register(EventType eventType,
                                                 Class<?> eventPayloadClass) {
    var alreadyRegisteredClass = map.putIfAbsent(eventType, eventPayloadClass);
    if (alreadyRegisteredClass != null) {
      throw new EntityEventsRegistrationException(
          "Event payload class must be registered exactly once, which is not the case for %s: registered both %s and %s"
              .formatted(eventType, alreadyRegisteredClass, eventPayloadClass));
    }

    return this;
  }

  public Optional<Class<?>> payloadClassFor(EventType eventType) {
    return Optional.ofNullable(map.get(eventType));
  }

  public Set<EventType> registeredEventTypes() {
    return map.keySet();
  }

  private static void validate(Class<?> eventPayloadClass) {
    if (isAbstract(eventPayloadClass)
        || isInterface(eventPayloadClass)
        || isArray(eventPayloadClass)
        || isPrimitive(eventPayloadClass)
        || isInnerNonStaticClass(eventPayloadClass)
        || isAnonymousClass(eventPayloadClass)
        || isLocalClass(eventPayloadClass)
        || isEnum(eventPayloadClass)
        || isJavaBuiltIn(eventPayloadClass)) {
      throw new EntityEventsRegistrationException(
          "Event payload class must be concrete class, which is not the case for %s"
              .formatted(eventPayloadClass));
    }
  }

  private static boolean isAbstract(Class<?> clazz) {
    return Modifier.isAbstract(clazz.getModifiers());
  }

  private static boolean isInterface(Class<?> clazz) {
    return clazz.isInterface();
  }

  private static boolean isArray(Class<?> clazz) {
    return clazz.isArray();
  }

  private static boolean isPrimitive(Class<?> clazz) {
    return clazz.isPrimitive();
  }

  private static boolean isInnerNonStaticClass(Class<?> clazz) {
    return clazz.isMemberClass() && !Modifier.isStatic(clazz.getModifiers());
  }

  private static boolean isAnonymousClass(Class<?> clazz) {
    return clazz.isAnonymousClass();
  }

  private static boolean isLocalClass(Class<?> clazz) {
    return clazz.isLocalClass();
  }

  private static boolean isEnum(Class<?> clazz) {
    return clazz.isEnum();
  }

  private static boolean isJavaBuiltIn(Class<?> clazz) {
    String name = clazz.getName();
    return name.startsWith("java.")
        || name.startsWith("javax.")
        || name.startsWith("sun.")
        || name.startsWith("com.sun.");
  }
}
