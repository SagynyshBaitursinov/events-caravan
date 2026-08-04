package io.saga.caravan.autoconfigure.dynamodb;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Configures the DynamoDB-backed {@code SnapshotStore} autoconfigured under the
 * {@value #PREFIX} prefix.
 *
 * @param tableName      the DynamoDB table snapshots are stored in
 * @param consistentRead whether to use DynamoDB strongly consistent reads when loading a
 *                       snapshot; defaults to {@code false} (eventually consistent reads), which
 *                       is cheaper and sufficient for most use cases
 */
@ConfigurationProperties(DynamoDbSnapshotStoreProperties.PREFIX)
public record DynamoDbSnapshotStoreProperties(String tableName,
                                              @DefaultValue("false") boolean consistentRead) {

  public static final String PREFIX = "caravan.event.sourcing.snapshot-store.dynamo-db";
}
