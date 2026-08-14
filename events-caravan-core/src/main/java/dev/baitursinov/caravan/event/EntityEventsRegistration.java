package dev.baitursinov.caravan.event;

import java.util.Map;

/**
 * Declares, for one entity type, every event name associated to it and the concrete payload class
 * carried by each.
 * <p>
 * Registered Entity does not have to be necessarily event sourced.
 * Registered Events may be produced by other applications and consumed in application being configured.
 * <p>
 * Applications supply a collection of these to
 * {@link EntityEventsRegistry#createFor(java.util.Collection)} to build the registry that
 * validates event production and event-sourcing apply methods.
 *
 * @param entityName          the entity type these events belong to
 * @param eventToPayloadClass maps each event name to its payload class
 */
public record EntityEventsRegistration(String entityName,
                                       Map<String, Class<?>> eventToPayloadClass) {
}
