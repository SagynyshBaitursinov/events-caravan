package io.saga.caravan.autoconfigure.dynamodb;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configures the DynamoDB-backed {@code SnapshotStore} autoconfigured under the
 * {@value #PREFIX} prefix.
 *
 * @param tableName the DynamoDB table snapshots are stored in
 */
@ConfigurationProperties(DynamoDbSnapshotStoreProperties.PREFIX)
public record DynamoDbSnapshotStoreProperties(String tableName) {

  public static final String PREFIX = "caravan.event.sourcing.snapshot-store.dynamo-db";
}
