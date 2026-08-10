package dev.baitursinov.caravan.event.sourcing.dynamodb.entity.stream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class Fnv1a64Test {

  @Test
  void matchesOfficialFnv1a64TestVectors() {
    assertThat(Fnv1a64.hash("")).isEqualTo(0xcbf29ce484222325L);
    assertThat(Fnv1a64.hash("a")).isEqualTo(0xaf63dc4c8601ec8cL);
    assertThat(Fnv1a64.hash("foobar")).isEqualTo(0x85944171f73967e8L);
  }

  @SuppressWarnings("EqualsWithItself")
  @Test
  void isDeterministic() {
    assertThat(Fnv1a64.hash("entity-1")).isEqualTo(Fnv1a64.hash("entity-1"));
  }

  @Test
  void differentInputsHashDifferently() {
    assertThat(Fnv1a64.hash("entity-1")).isNotEqualTo(Fnv1a64.hash("entity-2"));
  }
}
