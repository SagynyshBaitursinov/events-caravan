package dev.baitursinov.caravan.event.sourcing.applying;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares classes outside the entity class that hold {@link ApplyEvent} methods
 * for this entity, so apply logic can live outside the entity class, and entity class stays slimmer.
 * <p>
 * It's recommended to place {@link EventApplier} classes in same package where entity is located,
 * and make entity's fields or methods to mutate its state package private.
 * That way EventSourcedEntity's package will preserve encapsulation.
 *
 * <p>Each declared class may hold static methods with
 * {@code (EntityClass entity, Event<PayloadClass> event)} parameters, annotated with
 * {@link ApplyEvent}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface EventApplier {

  Class<?>[] value();
}
