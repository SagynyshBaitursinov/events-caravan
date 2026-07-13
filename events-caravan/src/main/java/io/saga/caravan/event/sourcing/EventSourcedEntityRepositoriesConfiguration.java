package io.saga.caravan.event.sourcing;

import io.saga.caravan.event.producer.EventProducer;
import io.saga.caravan.event.sourcing.snapshot.SnapshotStore;
import io.saga.caravan.event.sourcing.snapshot.SnapshotTaker;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import java.util.Map;

@Configuration
public class EventSourcedEntityRepositoriesConfiguration {

  @Bean
  public static BeanPostProcessor repositoryDependenciesSetter(
      @Lazy EventStore eventStore,
      @Lazy EventProducer eventProducer,
      @Lazy EventSourcedEntityNamesKeeper eventSourcedEntityNamesKeeper,
      @Lazy EntityEventApplier eventApplier,
      @Lazy SnapshotStore snapshotStore,
      @Lazy Map<Class<? extends EventSourcedEntity>, SnapshotTaker<? extends EventSourcedEntity, ?>> snapshotTakerMap) {

    return new BeanPostProcessor() {

      @Override
      public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        if (bean instanceof EventSourcedEntityRepository<?> eventSourcedEntityRepository) {
          eventSourcedEntityRepository.setEventStore(eventStore);
          eventSourcedEntityRepository.setEventProducer(eventProducer);
          eventSourcedEntityRepository.setEntityEventApplier(eventApplier);
          eventSourcedEntityRepository.setSnapshotStore(snapshotStore);
          eventSourcedEntityRepository.setSnapshotTaker(
              snapshotTakerMap.get(eventSourcedEntityRepository.entityClass()));

          eventSourcedEntityNamesKeeper.register(
              eventSourcedEntityRepository.entityName(),
              eventSourcedEntityRepository.entityClass());
        }
        return bean;
      }
    };
  }
}
