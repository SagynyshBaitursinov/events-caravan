package dev.baitursinov.caravan.event.publisher.sns;

import software.amazon.awssdk.services.sns.SnsClient;

/**
 * Resolves the SNS topic ARN that events of a given {@code entityName} are published to.
 */
@FunctionalInterface
public interface SnsTopicArnResolver {

  String topicArnFor(String entityName);

  /**
   * One topic per entity, {@code arn:${partition}:sns:${region}:${accountId}:${topicNamePrefix}_${entityName}},
   * the same convention the {@code dynamodb-sqs-events-publisher-lambda} publishes by.
   */
  static SnsTopicArnResolver byExpectation(String partition,
                                           String region,
                                           String accountId,
                                           String topicNamePrefix) {
    var topicArnPrefix = "arn:%s:sns:%s:%s:%s_".formatted(partition, region, accountId, topicNamePrefix);
    return entityName -> topicArnPrefix + entityName;
  }

  /**
   * One topic per entity named {@code ${topicNamePrefix}_${entityName}}, its ARN looked up by
   * name via {@code ListTopics}, the SNS counterpart of SQS {@code GetQueueUrl}: partition, region
   * and account id never have to be configured. Requires {@code sns:ListTopics}.
   *
   * @see TopicListingSnsTopicArnResolver
   */
  static SnsTopicArnResolver byLookup(SnsClient snsClient, String topicNamePrefix) {
    return new TopicListingSnsTopicArnResolver(snsClient, topicNamePrefix);
  }
}
