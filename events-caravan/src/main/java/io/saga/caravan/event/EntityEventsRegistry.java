package io.saga.caravan.event;

import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The authoritative map from every {@link EventType} an application declares to its payload
 * class. Built once at startup from the application's {@link EntityEventsRegistration}s, then
 * used to validate that produced events and event-sourcing apply methods carry the expected
 * payload type.
 */
public class EntityEventsRegistry {

  private static final Pattern ALLOWED_ENTITY_NAME_PATTERN = Pattern.compile("[A-Za-z0-9_-]+");

  private final Map<EventType, Class<?>> map = new HashMap<>();

  /**
   * Builds a registry from the given registrations.
   *
   * @throws EntityEventsRegistrationException if an entity name is invalid or duplicated, or an
   *                                           event's payload class is not a concrete class
   */
  public static EntityEventsRegistry createFor(
      Collection<EntityEventsRegistration> entityEventsRegistrations) {

    var result = new EntityEventsRegistry();
    var alreadyProcessedEntityNames = new HashSet<String>();

    entityEventsRegistrations.forEach(
        entityEventsRegistration -> {
          var entityName = entityEventsRegistration.entityName();

          validateEntityName(entityName);

          if (!alreadyProcessedEntityNames.add(entityName)) {
            throw new EntityEventsRegistrationException(
                "Registration for entityName=%s is duplicated".formatted(entityName));
          }

          entityEventsRegistration.eventToPayloadClass()
              .forEach((eventName, eventPayloadClass) -> {
                validate(eventPayloadClass);
                result.map.put(
                    new EventType(entityName, eventName), eventPayloadClass);
              });
        });

    return result;
  }

  /**
   * The payload class registered for the given event type, if any.
   */
  public Optional<Class<?>> payloadClassFor(EventType eventType) {
    return Optional.ofNullable(map.get(eventType));
  }

  /**
   * All event types known to this registry.
   */
  public Set<EventType> registeredEventTypes() {
    return map.keySet();
  }

  private static void validateEntityName(String entityName) {
    if (!ALLOWED_ENTITY_NAME_PATTERN.matcher(entityName).matches()) {
      throw new EntityEventsRegistrationException(
          "entityName must contain only alphanumerics, hyphens and underscores, got '%s'"
              .formatted(entityName));
    }
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
