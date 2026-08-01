package io.saga.caravan.queue.polling;

import java.util.Map;

public record Message(String id,
                      String body,
                      Map<String, String> metadata) {
}
