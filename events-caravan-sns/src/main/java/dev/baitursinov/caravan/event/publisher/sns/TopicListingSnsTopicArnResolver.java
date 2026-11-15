package dev.baitursinov.caravan.event.publisher.sns;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.ListTopicsRequest;
import software.amazon.awssdk.services.sns.model.Topic;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link SnsTopicArnResolver} finding the ARN of topic {@code ${topicNamePrefix}_${entityName}}
 * by paging through {@code ListTopics} until the topic is found, as SNS offers no lookup by name.
 * Only ARNs of resolved entities are cached; other listed topics are not kept. A missing topic is
 * not cached either, so it is picked up once created, and is never created by the resolver.
 */
@Slf4j
public class TopicListingSnsTopicArnResolver implements SnsTopicArnResolver {

  private static final String TOPIC_NAME_TEMPLATE = "%s_%s";

  private final SnsClient snsClient;
  private final String topicNamePrefix;
  private final Map<String, String> topicArnByEntityName = new ConcurrentHashMap<>();

  public TopicListingSnsTopicArnResolver(SnsClient snsClient,
                                         String topicNamePrefix) {
    this.snsClient = snsClient;
    this.topicNamePrefix = topicNamePrefix;
  }

  /**
   * @throws SnsTopicNotFoundException if no topic named {@code ${topicNamePrefix}_${entityName}} exists
   */
  @Override
  public String topicArnFor(String entityName) {
    return topicArnByEntityName.computeIfAbsent(entityName, this::lookUp);
  }

  private String lookUp(String entityName) {
    var topicArnSuffix = ":" + TOPIC_NAME_TEMPLATE.formatted(topicNamePrefix, entityName);

    var topicArn = snsClient.listTopicsPaginator(ListTopicsRequest.builder().build())
        .topics()
        .stream()
        .map(Topic::topicArn)
        .filter(arn -> arn.endsWith(topicArnSuffix))
        .findFirst()
        .orElseThrow(() -> new SnsTopicNotFoundException(
            "topicName=%s does not exist".formatted(topicArnSuffix.substring(1))));

    log.debug("Resolved entityName={} to topicArn={}", entityName, topicArn);
    return topicArn;
  }
}
