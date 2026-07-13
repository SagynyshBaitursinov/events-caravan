package io.saga.caravan.entity;

import java.util.Optional;

public interface Repository<T> {

  void save(T entity);

  Optional<T> findBy(String entityId);
}
