package io.saga.caravan.test.event.sourcing.entity.account;

import io.saga.caravan.event.Event;
import io.saga.caravan.event.sourcing.applying.ApplyEvent;
import io.saga.caravan.test.event.sourcing.entity.calculator.NumberCarryingPayload;
import lombok.NoArgsConstructor;

import static io.saga.caravan.test.event.sourcing.entity.account.AccountEventsConfiguration.RECEIVED_MONEY;

@NoArgsConstructor
public final class AccountEventApplier {

  @ApplyEvent(RECEIVED_MONEY)
  public static void applyReceivedMoney(Account account,
                                        Event<NumberCarryingPayload> numberCarryingPayloadEvent) {
    account.balance += numberCarryingPayloadEvent.payload().number();
  }
}
