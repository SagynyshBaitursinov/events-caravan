package io.saga.caravan.entity;

import java.util.Optional;

/**
 * Persists and retrieves entities of type {@code T} by their id.
 *
 * @param <T> the entity type this repository manages
 */
public interface Repository<T> {

  /**
   * Persists the given entity, creating or updating it as needed.
   */
  void save(T entity);

  /**
   * Retrieves the entity with the given id, if it exists.
   */
  Optional<T> findBy(String entityId);
}
