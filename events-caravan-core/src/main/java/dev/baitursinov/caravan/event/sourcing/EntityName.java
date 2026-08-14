package dev.baitursinov.caravan.event.sourcing;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares the entityName of an {@link EventSourcedEntity}.
 * <p>
 * The entityName is the single source of truth for the entity.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface EntityName {

  String value();
}
