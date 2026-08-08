package dev.baitursinov.caravan.event.sourcing.dynamodb;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DynamoDbUtils {

  public static void requireTableIsActive(DynamoDbClient dynamoDbClient, String tableName) {
    TableStatus tableStatus;
    try {
      tableStatus = dynamoDbClient
          .describeTable(DescribeTableRequest.builder().tableName(tableName).build())
          .table()
          .tableStatus();
    } catch (ResourceNotFoundException exception) {
      throw new DynamoDbSetupException(
          "tableName=%s does not exist".formatted(tableName), exception);
    }

    if (tableStatus != TableStatus.ACTIVE) {
      throw new DynamoDbSetupException(
          "tableName=%s must be ACTIVE, current status=%s".formatted(tableName, tableStatus));
    }
  }
}
