package dev.baitursinov.caravan.event.sourcing.entity.stream;

import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Holds map of entityName to its {@link EntityStreamRegistration}. Built once at
 * startup from an application's registrations, then consulted by
 * {@link EntityStreamWritingEventHandler} to decide whether, and at which location, an entity is
 * recorded into the entity stream.
 */
@Slf4j
public class EntityStreamRegistry {

  private final Map<String, EntityStreamRegistration> registrations = new HashMap<>();

  /**
   * Builds a registry from the given registrations.
   *
   * @throws EntityStreamRegistrationException if an entityName is registered more than once
   */
  public static EntityStreamRegistry createFor(
      Collection<EntityStreamRegistration> entityStreamRegistrations) {

    var result = new EntityStreamRegistry();

    entityStreamRegistrations.forEach(registration -> {
      var entityName = registration.entityName();

      if (result.registrations.putIfAbsent(entityName, registration) != null) {
        throw new EntityStreamRegistrationException(
            "Registration for entityName=%s is duplicated".formatted(entityName));
      }

      log.debug(
          "Registered entityName={} for the entity stream with timeBucket={}, shardCount={}",
          entityName, registration.timeBucket(), registration.shardCount());
    });

    log.info("Built EntityStreamRegistry with {} entityName(s)", result.registrations.size());

    return result;
  }

  /**
   * The registration for the given entityName, if any.
   */
  public Optional<EntityStreamRegistration> registrationFor(String entityName) {
    return Optional.ofNullable(registrations.get(entityName));
  }
}
