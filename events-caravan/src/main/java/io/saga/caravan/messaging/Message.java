package io.saga.caravan.messaging;

import java.util.Map;

public record Message(String id,
                      String body,
                      Map<String, String> metadata) {
}
