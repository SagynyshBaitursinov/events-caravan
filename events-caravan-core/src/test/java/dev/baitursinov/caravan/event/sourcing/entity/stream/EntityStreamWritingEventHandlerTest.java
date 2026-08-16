package dev.baitursinov.caravan.event.sourcing.entity.stream;

import dev.baitursinov.caravan.entity.EntityReference;
import dev.baitursinov.caravan.event.Event;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class EntityStreamWritingEventHandlerTest {

  public static final String EVENT_TIMESTAMP = "2026-08-10T14:03:22.123Z";
  public static final String ENTITY_ID = "1";

  record WrittenEntry(EntityStreamEntry entry, String timeBucketLocation, int shardLocation) {
  }

  static class InMemoryEntityStreamWriter implements EntityStreamWriter {

    final List<WrittenEntry> writtenEntities = new ArrayList<>();

    @Override
    public void write(EntityStreamEntry entry, String timeBucketLocation, int shardLocation) {
      writtenEntities.add(new WrittenEntry(entry, timeBucketLocation, shardLocation));
    }
  }

  InMemoryEntityStreamWriter entityStreamWriter = new InMemoryEntityStreamWriter();
  EntityStreamRegistry entityStreamRegistry = EntityStreamRegistry.createFor(
      List.of(new EntityStreamRegistration("car", TimeBucket.MONTHLY, 16)));
  EntityStreamWritingEventHandler handler =
      new EntityStreamWritingEventHandler(entityStreamWriter, entityStreamRegistry);

  @Test
  void isOfInterestOnlyInFirstEvent() {
    assertThat(handler.isOfInterest(event("car", 1))).isTrue();
    assertThat(handler.isOfInterest(event("car", 2))).isFalse();
  }

  @Test
  void isNotOfInterestWhenEntityNameIsNotRegistered() {
    assertThat(handler.isOfInterest(event("unregistered-entity", 1))).isFalse();
    assertThat(handler.isOfInterest(event("unregistered-entity", 2))).isFalse();
  }

  @Test
  void writesEntityFromFirstEvent() {
    var event = event("car", 1);
    var expectedShardLocation = Math.floorMod(Fnv1a64.hash(ENTITY_ID), 16);

    handler.handle(event);

    assertThat(entityStreamWriter.writtenEntities)
        .containsExactly(
            new WrittenEntry(
                new EntityStreamEntry(event.entityReference(), event.timestamp()),
                "2026-08",
                expectedShardLocation));
  }

  private Event<Object> event(String entityName, long sequenceNumber) {
    return Event.builder()
        .entityReference(new EntityReference(entityName, ENTITY_ID))
        .eventName("turned-on")
        .sequenceNumber(sequenceNumber)
        .timestamp(ZonedDateTime.parse(EVENT_TIMESTAMP))
        .payload("payload")
        .build();
  }
}
