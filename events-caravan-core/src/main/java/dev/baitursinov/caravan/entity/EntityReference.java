package dev.baitursinov.caravan.entity;

import static dev.baitursinov.caravan.utils.TextUtils.hasText;

/**
 * Identifies a single {@link Entity} instance by the name of its type and its id.
 */
public record EntityReference(String entityName,
                              String entityId) {

  public EntityReference {
    if (!hasText(entityName) || !hasText(entityId)) {
      throw new IllegalArgumentException(
          "EntityReference must include entityName and entityId");
    }
  }

  @Override
  public String toString() {
    return "Entity{" + this.entityName() + ":" + this.entityId() + "}";
  }
}
