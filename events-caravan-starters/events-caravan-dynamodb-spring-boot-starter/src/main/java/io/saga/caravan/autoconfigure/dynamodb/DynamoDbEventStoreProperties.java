package io.saga.caravan.autoconfigure.dynamodb;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(DynamoDbEventStoreProperties.PREFIX)
public record DynamoDbEventStoreProperties(String tableName,
                                           @DefaultValue("1000") int queryMaxPageSize,
                                           @DefaultValue("10000") long partitionShardSize) {

  public static final String PREFIX = "caravan.event.store.dynamo-db";
}
