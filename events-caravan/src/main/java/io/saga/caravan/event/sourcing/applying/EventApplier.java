package io.saga.caravan.event.sourcing.applying;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares classes outside the entity class that hold {@link ApplyEvent} methods
 * for this entity, so apply logic can live outside the entity class, and entity class stays slimmer.
 * It's recommended to place EventAppliers in same package as EventSourcedEntity,
 * and make entity's fields or methods to mutate its state package private.
 * That way EventSourcedEntity's package will preserve encapsulation.
 *
 * <p>Each declared class may hold static methods with
 * {@code (EntityClass entity, Event<PayloadClass> event)} parameters, annotated with
 * {@link ApplyEvent}. Declarations on superclasses of the entity are collected as well.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface EventApplier {

  Class<?>[] value();
}
