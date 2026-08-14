package dev.baitursinov.caravan.event;

import static dev.baitursinov.caravan.utils.TextUtils.hasText;

/**
 * Identifies a kind of event: the event named {@code eventName} as it occurs for entities named
 * {@code entityName}.
 */
public record EventType(String entityName,
                        String eventName) {

  public EventType {
    if (!hasText(entityName) || !hasText(eventName)) {
      throw new IllegalArgumentException(
          "EventType must include entityName and eventName");
    }
  }
}
