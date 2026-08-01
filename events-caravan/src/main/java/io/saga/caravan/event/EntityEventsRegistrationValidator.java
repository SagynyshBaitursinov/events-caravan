package io.saga.caravan.event;

import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.regex.Pattern;

@RequiredArgsConstructor
public class EntityEventsRegistrationValidator {

  private static final Pattern ALLOWED_ENTITY_NAME = Pattern.compile("[A-Za-z0-9_-]+");
  private static final int MAX_ENTITY_NAME_LENGTH = 64;

  private final Collection<EntityEventsRegistration> entityEventsRegistrations;

  public void validateAll() {
    entityEventsRegistrations.stream()
        .map(EntityEventsRegistration::entityName)
        .forEach(this::validate);
  }

  private void validate(String entityName) {
    if (!ALLOWED_ENTITY_NAME.matcher(entityName).matches()) {
      throw new EntityEventsRegistrationException(
          "entityName must contain only alphanumerics, hyphens and underscores, got '%s'"
              .formatted(entityName));
    }

    if (entityName.length() > MAX_ENTITY_NAME_LENGTH) {
      throw new EntityEventsRegistrationException(
          "entityName must not be longer than %d characters, got '%s' of %d"
              .formatted(MAX_ENTITY_NAME_LENGTH, entityName, entityName.length()));
    }
  }
}
