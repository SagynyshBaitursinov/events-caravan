package io.saga.caravan.test.event.sourcing;

import io.saga.caravan.test.AbstractSpringBootTest;
import io.saga.caravan.test.event.sourcing.entity.account.Account;
import io.saga.caravan.test.event.sourcing.entity.account.AccountRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class ExternalEventAppliersTest extends AbstractSpringBootTest {

  @Autowired
  AccountRepository accountRepository;

  @Test
  void shouldUseExternalApplierToApplyEvents() {
    String accountId = UUID.randomUUID().toString();
    var account = Account.createNew(accountId, 33);
    account.addMoney(67);
    assertThat(account.getBalance()).isEqualTo(100L);
    assertThat(account.version()).isEqualTo(2);

    accountRepository.save(account);

    var loadedAccount = accountRepository.findBy(accountId).orElseThrow();
    assertThat(loadedAccount.getBalance()).isEqualTo(100);
    assertThat(loadedAccount.version()).isEqualTo(2);
  }
}
