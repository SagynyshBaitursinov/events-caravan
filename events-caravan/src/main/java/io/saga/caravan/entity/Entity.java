package io.saga.caravan.entity;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Base class for a domain entity, identified by an {@link #entityName()} and {@link #entityId()}.
 *
 * <p>Applications extend this class for each kind of entity in their domain model.
 * {@code equals}/{@code hashCode} are defined in terms of {@link #entityReference()} and are
 * {@code final}, so identity is always based on entity type and id, never on other fields.
 */
public abstract class Entity {

  /**
   * The identifier of this entity instance, must be unique within {@link #entityName()}.
   */
  public abstract String entityId();

  /**
   * The name of this entity's type, must be unique.
   */
  public abstract String entityName();

  public final EntityReference entityReference() {
    return new EntityReference(this.entityName(), this.entityId());
  }

  @Override
  public String toString() {
    return this.entityReference().toString();
  }

  @Override
  public final boolean equals(@Nullable Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    Entity entity = (Entity) o;
    return Objects.equals(this.entityReference(), entity.entityReference());
  }

  @Override
  public final int hashCode() {
    return Objects.hash(this.entityReference());
  }
}
