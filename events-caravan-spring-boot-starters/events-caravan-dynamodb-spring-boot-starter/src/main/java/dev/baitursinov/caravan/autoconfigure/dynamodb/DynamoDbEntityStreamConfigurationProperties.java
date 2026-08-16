package dev.baitursinov.caravan.autoconfigure.dynamodb;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configures the DynamoDB-backed entity stream autoconfigured under the
 * {@value #PREFIX} prefix. The feature is enabled by configuring it: an absent
 * {@code tableName} means no entity stream beans are created.
 *
 * @param tableName the DynamoDB table entities are written in stream
 */
@ConfigurationProperties(DynamoDbEntityStreamConfigurationProperties.PREFIX)
public record DynamoDbEntityStreamConfigurationProperties(String tableName) {

  public static final String PREFIX = "caravan.event.sourcing.entity-stream.dynamo-db";

}
