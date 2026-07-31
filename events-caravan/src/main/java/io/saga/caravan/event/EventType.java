package io.saga.caravan.event;

import static io.saga.caravan.utils.TextUtils.hasText;

public record EventType(String entityName,
                        String eventName) {

  public EventType {
    if (!hasText(entityName) || !hasText(eventName)) {
      throw new IllegalArgumentException(
          "EventType must include entityName and eventName");
    }
  }
}
