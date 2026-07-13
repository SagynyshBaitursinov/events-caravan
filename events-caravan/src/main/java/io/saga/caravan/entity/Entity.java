package io.saga.caravan.entity;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

public abstract class Entity {

  public abstract String entityId();

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
