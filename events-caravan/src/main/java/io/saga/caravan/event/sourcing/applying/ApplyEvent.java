package io.saga.caravan.event.sourcing.applying;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Marks a method that applies an event to an {@code EventSourcedEntity}.
 *
 * <p>Two placements are supported:
 * <ul>
 *   <li>Inside the entity class (or its superclass) — an instance method with a single
 *       {@code Event<PayloadClass>} parameter.</li>
 *   <li>Inside a class declared in the entity's {@link ApplyEventSources} — a static method
 *       with {@code (EntityClass entity, Event<PayloadClass> event)} parameters, so apply
 *       logic can live outside the entity class.</li>
 * </ul>
 *
 * <p>Each event name must be handled by exactly one method across both placements.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface ApplyEvent {

  String value();
}
