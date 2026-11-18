package dev.baitursinov.caravan.autoconfigure;

import dev.baitursinov.caravan.event.EntityEventsRegistry;
import dev.baitursinov.caravan.event.producer.SequencedEventProducer;
import dev.baitursinov.caravan.event.producer.SequencedEventWriter;
import dev.baitursinov.caravan.event.producer.UnsequencedEventProducer;
import dev.baitursinov.caravan.event.producer.UnsequencedEventWriter;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
public class CaravanEventProducersAutoConfiguration {

  @Bean
  @ConditionalOnBean(SequencedEventWriter.class)
  @ConditionalOnMissingBean
  public SequencedEventProducer sequencedEventProducer(ObjectProvider<SequencedEventWriter> sequencedEventWriters,
                                                       EntityEventsRegistry entityEventsRegistry) {
    return new SequencedEventProducer(
        single(sequencedEventWriters, SequencedEventWriter.class), entityEventsRegistry);
  }

  @Bean
  @ConditionalOnBean(UnsequencedEventWriter.class)
  @ConditionalOnMissingBean
  public UnsequencedEventProducer unsequencedEventProducer(ObjectProvider<UnsequencedEventWriter> unsequencedEventWriters,
                                                           EntityEventsRegistry entityEventsRegistry) {
    return new UnsequencedEventProducer(
        single(unsequencedEventWriters, UnsequencedEventWriter.class), entityEventsRegistry);
  }

  private static <T> T single(ObjectProvider<T> candidates, Class<T> type) {
    var beans = candidates.stream().toList();
    if (beans.size() != 1) {
      throw new IllegalStateException(
          "Exactly one %s is expected, but found %s"
              .formatted(type.getSimpleName(), beans.stream().map(bean -> bean.getClass().getName()).toList()));
    }
    return beans.getFirst();
  }
}
