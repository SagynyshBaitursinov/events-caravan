package dev.baitursinov.caravan.event.sourcing.entity.stream;

import dev.baitursinov.caravan.entity.EntityReference;
import dev.baitursinov.caravan.event.Event;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class EntityStreamWritingEventHandlerTest {

  static class InMemoryEntityStreamWriter implements EntityStreamWriter {

    final List<EntityStreamEntry> writtenEntities = new ArrayList<>();

    @Override
    public void write(EntityStreamEntry entry) {
      writtenEntities.add(entry);
    }
  }

  private final InMemoryEntityStreamWriter entityStreamWriter = new InMemoryEntityStreamWriter();
  private final EntityStreamWritingEventHandler handler =
      new EntityStreamWritingEventHandler(entityStreamWriter);

  @Test
  void isOfInterestOnlyInFirstEvent() {
    assertThat(handler.isOfInterest(event(1))).isTrue();
    assertThat(handler.isOfInterest(event(2))).isFalse();
  }

  @Test
  void writesEntityFromFirstEvent() {
    var event = event(1);

    handler.handle(event);

    assertThat(entityStreamWriter.writtenEntities)
        .containsExactly(new EntityStreamEntry(event.entityReference(), event.timestamp()));
  }

  private Event<Object> event(long sequenceNumber) {
    return Event.builder()
        .entityReference(new EntityReference("car", "1"))
        .eventName("turned-on")
        .sequenceNumber(sequenceNumber)
        .timestamp(ZonedDateTime.now())
        .payload("payload")
        .build();
  }
}
