package dev.baitursinov.caravan.test.event.sourcing.entity.account;

import dev.baitursinov.caravan.event.sourcing.EntityName;
import dev.baitursinov.caravan.event.sourcing.EventSourcedEntity;
import dev.baitursinov.caravan.event.sourcing.applying.EventApplier;
import dev.baitursinov.caravan.test.event.sourcing.entity.calculator.NumberCarryingPayload;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import static dev.baitursinov.caravan.test.event.sourcing.entity.account.AccountEventsConfiguration.ACCOUNT;
import static dev.baitursinov.caravan.test.event.sourcing.entity.account.AccountEventsConfiguration.RECEIVED_MONEY;

@EntityName(ACCOUNT)
@EventApplier(AccountEventApplier.class)
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class Account extends EventSourcedEntity {

  private final String accountId;

  @Getter
  long balance;

  public static Account createNew(String id, long initialBalance) {
    var account = new Account(id);
    account.addMoney(initialBalance);
    return account;
  }

  @Override
  public String entityId() {
    return accountId;
  }

  public void addMoney(long amount) {
    recordEvent(RECEIVED_MONEY, new NumberCarryingPayload(amount));
  }
}
