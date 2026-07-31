package io.saga.caravan.entity;

import static io.saga.caravan.utils.TextUtils.hasText;

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
