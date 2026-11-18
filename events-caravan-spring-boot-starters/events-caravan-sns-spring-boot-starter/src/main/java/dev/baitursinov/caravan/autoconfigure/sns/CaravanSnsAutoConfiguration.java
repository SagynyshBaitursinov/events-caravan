package dev.baitursinov.caravan.autoconfigure.sns;

import dev.baitursinov.caravan.autoconfigure.CaravanMessagePublishingAutoConfiguration;
import dev.baitursinov.caravan.event.publisher.EventMessagePublisher;
import dev.baitursinov.caravan.event.publisher.sns.SnsEventMessagePublisher;
import dev.baitursinov.caravan.event.publisher.sns.SnsTopicArnResolver;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import software.amazon.awssdk.services.sns.SnsClient;

@AutoConfiguration(before = CaravanMessagePublishingAutoConfiguration.class)
@EnableConfigurationProperties(CaravanSnsPublishingConfigurationProperties.class)
public class CaravanSnsAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  public SnsTopicArnResolver snsTopicArnResolver(SnsClient snsClient,
                                                 CaravanSnsPublishingConfigurationProperties properties) {
    var topicNamePrefix = properties.topicNamePrefix();
    if (topicNamePrefix == null || topicNamePrefix.isBlank()) {
      throw new SnsTopicsSetupException("topic-name-prefix must be present");
    }

    return SnsTopicArnResolver.byLookup(snsClient, topicNamePrefix);
  }

  @Bean
  @ConditionalOnMissingBean
  public EventMessagePublisher snsEventMessagePublisher(SnsClient snsClient,
                                                        SnsTopicArnResolver snsTopicArnResolver) {
    return new SnsEventMessagePublisher(snsClient, snsTopicArnResolver);
  }
}
