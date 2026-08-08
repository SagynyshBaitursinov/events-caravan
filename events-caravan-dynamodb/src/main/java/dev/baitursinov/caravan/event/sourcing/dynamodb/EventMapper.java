package dev.baitursinov.caravan.event.sourcing.dynamodb;

import dev.baitursinov.caravan.event.Event;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Map;

public interface EventMapper {

  Event<?> mapAttributesToEvent(Map<String, AttributeValue> attributeValues);
}
