package dev.baitursinov.caravan.event.publisher.sns;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.ListTopicsRequest;
import software.amazon.awssdk.services.sns.model.ListTopicsResponse;
import software.amazon.awssdk.services.sns.model.Topic;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TopicListingSnsTopicArnResolverTest {

  private static final String CAR_TOPIC_ARN = "arn:aws:sns:eu-west-1:123456789012:events_car";
  private static final String BOAT_TOPIC_ARN = "arn:aws:sns:eu-west-1:123456789012:events_boat";

  SnsClient snsClient = mock(SnsClient.class);

  SnsTopicArnResolver resolver = SnsTopicArnResolver.byLookup(snsClient, "events");

  @Test
  void shouldStopListingOnceTopicIsFound() {
    listing(
        page(CAR_TOPIC_ARN),
        page(BOAT_TOPIC_ARN));

    assertThat(resolver.topicArnFor("car")).isEqualTo(CAR_TOPIC_ARN);

    verify(snsClient, times(1)).listTopics(any(ListTopicsRequest.class));
  }

  @Test
  void shouldPageThroughUntilTopicIsFoundAndCacheOnlyIt() {
    listing(
        page("arn:aws:sns:eu-west-1:123456789012:other_car", "arn:aws:sns:eu-west-1:123456789012:events_car_x"),
        page(CAR_TOPIC_ARN));

    assertThat(resolver.topicArnFor("car")).isEqualTo(CAR_TOPIC_ARN);
    assertThat(resolver.topicArnFor("car")).isEqualTo(CAR_TOPIC_ARN);

    verify(snsClient, times(2)).listTopics(any(ListTopicsRequest.class));
  }

  @Test
  void shouldListAgainForAnotherEntity() {
    listing(page(CAR_TOPIC_ARN, BOAT_TOPIC_ARN));
    assertThat(resolver.topicArnFor("car")).isEqualTo(CAR_TOPIC_ARN);

    listing(page(CAR_TOPIC_ARN, BOAT_TOPIC_ARN));
    assertThat(resolver.topicArnFor("boat")).isEqualTo(BOAT_TOPIC_ARN);

    verify(snsClient, times(2)).listTopics(any(ListTopicsRequest.class));
  }

  @Test
  void shouldFailForMissingTopicWithoutCachingTheMiss() {
    listing(page(CAR_TOPIC_ARN));

    assertThatThrownBy(() -> resolver.topicArnFor("boat"))
        .isInstanceOf(SnsTopicNotFoundException.class)
        .hasMessageContaining("events_boat");

    listing(page(CAR_TOPIC_ARN, BOAT_TOPIC_ARN));
    assertThat(resolver.topicArnFor("boat")).isEqualTo(BOAT_TOPIC_ARN);
  }

  private void listing(ListTopicsResponse... pages) {
    when(snsClient.listTopicsPaginator(any(ListTopicsRequest.class))).thenCallRealMethod();
    var stubbing = when(snsClient.listTopics(any(ListTopicsRequest.class)));
    for (int index = 0; index < pages.length; index++) {
      var page = index < pages.length - 1
          ? pages[index].toBuilder().nextToken("page-" + (index + 1)).build()
          : pages[index];
      stubbing = stubbing.thenReturn(page);
    }
  }

  private static ListTopicsResponse page(String... topicArns) {
    return ListTopicsResponse.builder()
        .topics(Arrays.stream(topicArns).map(arn -> Topic.builder().topicArn(arn).build()).toList())
        .build();
  }
}
