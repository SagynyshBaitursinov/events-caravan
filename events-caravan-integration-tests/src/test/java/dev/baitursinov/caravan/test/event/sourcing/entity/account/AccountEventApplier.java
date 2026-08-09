package dev.baitursinov.caravan.test.event.sourcing.entity.account;

import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.sourcing.applying.ApplyEvent;
import dev.baitursinov.caravan.test.value.NumberCarryingPayload;
import lombok.NoArgsConstructor;

import static dev.baitursinov.caravan.test.event.registration.AccountEventsConfiguration.RECEIVED_MONEY;

@NoArgsConstructor
public final class AccountEventApplier {

  @ApplyEvent(RECEIVED_MONEY)
  public static void applyReceivedMoney(Account account,
                                        Event<NumberCarryingPayload> numberCarryingPayloadEvent) {
    account.balance += numberCarryingPayloadEvent.payload().number();
  }
}
