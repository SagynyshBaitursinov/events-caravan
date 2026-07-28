package io.saga.caravan.event.sourcing.dynamodb;

import io.saga.caravan.event.Event;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Map;

public interface EventMapper {

  Event<?> mapAttributesToEvent(Map<String, AttributeValue> attributeValues);
}
