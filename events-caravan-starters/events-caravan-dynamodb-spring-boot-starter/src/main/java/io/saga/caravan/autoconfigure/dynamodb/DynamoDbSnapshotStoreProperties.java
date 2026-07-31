package io.saga.caravan.autoconfigure.dynamodb;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(DynamoDbSnapshotStoreProperties.PREFIX)
public record DynamoDbSnapshotStoreProperties(String tableName) {

  public static final String PREFIX = "caravan.event.sourcing.snapshot-store.dynamo-db";
}
