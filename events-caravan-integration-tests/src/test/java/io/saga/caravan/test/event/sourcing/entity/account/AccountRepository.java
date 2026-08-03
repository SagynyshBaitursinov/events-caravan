package io.saga.caravan.test.event.sourcing.entity.account;

import io.saga.caravan.event.sourcing.EventSourcedRepository;
import io.saga.caravan.event.sourcing.EventSourcingRepositoryContext;
import org.springframework.stereotype.Repository;

@Repository
public class AccountRepository extends EventSourcedRepository<Account> {

  public AccountRepository(EventSourcingRepositoryContext context) {
    super(Account.class, context);
  }

  @Override
  protected Account createWithBlankState(String entityId) {
    return new Account(entityId);
  }
}
