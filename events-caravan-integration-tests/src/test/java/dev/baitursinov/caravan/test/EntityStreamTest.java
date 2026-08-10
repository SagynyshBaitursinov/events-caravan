package dev.baitursinov.caravan.test;

import dev.baitursinov.caravan.entity.EntityReference;
import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.producer.EventProducer;
import dev.baitursinov.caravan.event.sourcing.entity.stream.EntityStreamEntry;
import dev.baitursinov.caravan.event.sourcing.entity.stream.EntityStreamWriter;
import dev.baitursinov.caravan.test.value.TestEventPayload;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static dev.baitursinov.caravan.test.event.registration.TestEntityEventsConfiguration.TEST_ENTITY;
import static dev.baitursinov.caravan.test.event.registration.TestEntityEventsConfiguration.TEST_EVENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class EntityStreamTest extends AbstractSpringBootTest {

  @Autowired
  EntityStreamWriter entityStreamWriter;

  @Autowired
  EventProducer eventProducer;

  @Autowired
  DynamoDbClient dynamoDbClient;

  @Value("${caravan.event.sourcing.entity-stream.dynamo-db.table-name}")
  String entityStreamTableName;

  @Test
  void writesAStreamItemKeyedByEntityNameAndTimeBucketAndShard() {
    var entityId = UUID.randomUUID().toString();
    var entityReference = new EntityReference("calculator", entityId);
    var firstEventTimestamp = ZonedDateTime.parse("2026-08-10T14:03:22.123Z");

    entityStreamWriter.write(new EntityStreamEntry(entityReference, firstEventTimestamp));

    var item = onlyWrittenStreamItemFor(entityId);
    assertThat(item.get("PK").s()).matches("calculator#2026-08#\\d+");
    assertThat(item.get("SK").s())
        .isEqualTo("2026-08-10T14:03:22.123Z#" + entityId);
  }

  @Test
  void writingTheSameEntityTwiceStaysIdempotent() {
    var entry = new EntityStreamEntry(
        new EntityReference("calculator", UUID.randomUUID().toString()), ZonedDateTime.now());

    entityStreamWriter.write(entry);
    entityStreamWriter.write(entry);

    assertThat(writtenStreamItemsFor(entry.entityReference().entityId())).hasSize(1);
  }

  @Test
  void writesAnEntityOnItsFirstEventThroughTheRealPipeline() {
    var entityId = UUID.randomUUID().toString();

    eventProducer.produce(
        Event.builder()
            .entityReference(new EntityReference(TEST_ENTITY, entityId))
            .eventName(TEST_EVENT)
            .sequenceNumber(1)
            .timestamp(ZonedDateTime.now())
            .payload(new TestEventPayload("write-me-to-stream", "please"))
            .build());

    await()
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofMillis(50))
        .untilAsserted(() ->
            assertThat(onlyWrittenStreamItemFor(entityId).get("PK").s())
                .startsWith(TEST_ENTITY + "#"));
  }

  private Map<String, AttributeValue> onlyWrittenStreamItemFor(String entityId) {
    var items = writtenStreamItemsFor(entityId);
    assertThat(items).hasSize(1);
    return items.getFirst();
  }

  private List<Map<String, AttributeValue>> writtenStreamItemsFor(String entityId) {
    return dynamoDbClient.scan(
            ScanRequest.builder()
                .tableName(entityStreamTableName)
                .filterExpression("contains(SK, :entityId)")
                .expressionAttributeValues(
                    Map.of(":entityId", AttributeValue.fromS(entityId)))
                .build())
        .items();
  }
}
