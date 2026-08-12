package dev.baitursinov.caravan.autoconfigure.dynamodb;

import dev.baitursinov.caravan.event.sourcing.dynamodb.entity.stream.TimeBucket;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Configures the DynamoDB-backed entity stream autoconfigured under the
 * {@value #PREFIX} prefix. The feature is enabled by configuring it: an absent
 * {@code tableName} means no entity stream beans are created.
 *
 * @param tableName  the DynamoDB table entities are written in stream
 * @param shardCount the number of shards each (entityName, time bucket) is split into;
 *                   must be immutable after first use
 * @param timeBucket the granularity entities are bucketed by creation time; must be immutable after
 *                   first use
 */
@ConfigurationProperties(DynamoDbEntityStreamConfigurationProperties.PREFIX)
public record DynamoDbEntityStreamConfigurationProperties(String tableName,
                                                          @DefaultValue("MONTHLY") TimeBucket timeBucket,
                                                          @DefaultValue("16") int shardCount) {

  public static final String PREFIX = "caravan.event.sourcing.entity-stream.dynamo-db";

}
