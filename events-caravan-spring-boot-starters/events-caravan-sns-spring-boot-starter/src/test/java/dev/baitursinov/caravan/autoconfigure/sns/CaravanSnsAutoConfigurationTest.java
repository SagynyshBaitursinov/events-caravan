package dev.baitursinov.caravan.autoconfigure.sns;

import dev.baitursinov.caravan.event.publisher.EventMessagePublisher;
import dev.baitursinov.caravan.event.publisher.sns.SnsEventMessagePublisher;
import dev.baitursinov.caravan.event.publisher.sns.SnsTopicArnResolver;
import dev.baitursinov.caravan.event.publisher.sns.TopicListingSnsTopicArnResolver;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.ListTopicsRequest;
import software.amazon.awssdk.services.sns.model.ListTopicsResponse;
import software.amazon.awssdk.services.sns.model.Topic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CaravanSnsAutoConfigurationTest {

  ApplicationContextRunner contextRunner = new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(CaravanSnsAutoConfiguration.class))
      .withUserConfiguration(ApplicationConfiguration.class)
      .withPropertyValues("caravan.event.publishing.sns.topic-name-prefix=test-app");

  @Configuration(proxyBeanMethods = false)
  static class ApplicationConfiguration {

    @Bean
    SnsClient snsClient() {
      var snsClient = mock(SnsClient.class);
      when(snsClient.listTopicsPaginator(any(ListTopicsRequest.class))).thenCallRealMethod();
      when(snsClient.listTopics(any(ListTopicsRequest.class)))
          .thenReturn(ListTopicsResponse.builder()
              .topics(Topic.builder().topicArn("arn:aws:sns:eu-west-1:123456789012:test-app_calculator").build())
              .build());
      return snsClient;
    }
  }

  @Test
  void shouldAutoconfigureSnsPublisherResolvingTopicsByPrefixedName() {
    contextRunner.run(context -> {
      assertThat(context).hasSingleBean(EventMessagePublisher.class);
      assertThat(context.getBean(EventMessagePublisher.class)).isInstanceOf(SnsEventMessagePublisher.class);
      assertThat(context.getBean(SnsTopicArnResolver.class)).isInstanceOf(TopicListingSnsTopicArnResolver.class);
      assertThat(context.getBean(SnsTopicArnResolver.class).topicArnFor("calculator"))
          .isEqualTo("arn:aws:sns:eu-west-1:123456789012:test-app_calculator");
    });
  }

  @Test
  void shouldFailWhenTopicNamePrefixIsMissing() {
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(CaravanSnsAutoConfiguration.class))
        .withUserConfiguration(ApplicationConfiguration.class)
        .run(context -> assertThat(context)
            .hasFailed()
            .getFailure()
            .hasRootCauseInstanceOf(SnsTopicsSetupException.class)
            .rootCause()
            .hasMessageContaining("topic-name-prefix"));
  }

  @Test
  void shouldFailWhenTopicNamePrefixIsBlank() {
    contextRunner
        .withPropertyValues("caravan.event.publishing.sns.topic-name-prefix= ")
        .run(context -> assertThat(context).hasFailed()
            .getFailure()
            .hasRootCauseInstanceOf(SnsTopicsSetupException.class));
  }

  @Test
  void shouldFailWithoutSnsClient() {
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(CaravanSnsAutoConfiguration.class))
        .withPropertyValues("caravan.event.publishing.sns.topic-name-prefix=test-app")
        .run(context -> assertThat(context).hasFailed());
  }

  @Test
  void shouldBackOffWhenApplicationProvidesOwnBeans() {
    var ownResolver = (SnsTopicArnResolver) entityName -> "arn:own:" + entityName;
    var ownPublisher = mock(EventMessagePublisher.class);

    contextRunner
        .withBean(SnsTopicArnResolver.class, () -> ownResolver)
        .withBean(EventMessagePublisher.class, () -> ownPublisher)
        .run(context -> {
          assertThat(context.getBean(SnsTopicArnResolver.class)).isSameAs(ownResolver);
          assertThat(context.getBean(EventMessagePublisher.class)).isSameAs(ownPublisher);
        });
  }

  @Test
  void shouldUseOwnResolverInAutoconfiguredPublisher() {
    contextRunner
        .withBean(SnsTopicArnResolver.class, () -> entityName -> "arn:own:" + entityName)
        .run(context -> {
          assertThat(context).hasSingleBean(EventMessagePublisher.class);
          assertThat(context.getBean(EventMessagePublisher.class)).isInstanceOf(SnsEventMessagePublisher.class);
        });
  }
}
