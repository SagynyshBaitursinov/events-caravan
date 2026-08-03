package io.saga.caravan.autoconfigure.dynamodb;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Configures the DynamoDB-backed {@code EventStore} autoconfigured under the
 * {@value #PREFIX} prefix.
 *
 * @param tableName          the DynamoDB table events are stored in
 * @param queryMaxPageSize   the maximum number of items requested per query page when reading
 *                           an entity's events to avoid running out of memory
 * @param partitionShardSize the number of events stored per partition-key shard, to shard
 *                           entities event history across multiple partitions
 */
@ConfigurationProperties(DynamoDbEventStoreProperties.PREFIX)
public record DynamoDbEventStoreProperties(String tableName,
                                           @DefaultValue("1000") int queryMaxPageSize,
                                           @DefaultValue("10000") long partitionShardSize) {

  public static final String PREFIX = "caravan.event.store.dynamo-db";
}
