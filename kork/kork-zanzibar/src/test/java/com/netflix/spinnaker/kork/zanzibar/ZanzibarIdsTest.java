/*
 * Copyright 2026 Apple, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.kork.zanzibar;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.function.IntPredicate;
import org.junit.jupiter.api.Test;

class ZanzibarIdsTest {

  /** SpiceDB's object id charset, minus the {@code =} escape. */
  private static final IntPredicate SPICEDB =
      c ->
          (c >= 'a' && c <= 'z')
              || (c >= 'A' && c <= 'Z')
              || (c >= '0' && c <= '9')
              || c == '/'
              || c == '_'
              || c == '|'
              || c == '-'
              || c == '+';

  private static final List<String> IDS =
      List.of(
          "jane.doe@example.com",
          "urn:example:person:1234567",
          "urn:example:group:7654321",
          "prod.us-west-2",
          "a=b",
          "=3A",
          "日本",
          "1234567");

  @Test
  void roundTripsIntoTheSpiceDbCharset() {
    for (var id : IDS) {
      var encoded = ZanzibarIds.encode(id, SPICEDB);
      assertThat(encoded).matches("^([a-zA-Z0-9/_|\\-=+]{1,})$");
      assertThat(ZanzibarIds.decode(encoded)).isEqualTo(id);
    }
  }

  @Test
  void escapesOnlyWhatTheEngineRejects() {
    IntPredicate noColon = c -> c > 0x20 && c < 0x7F && c != ':' && c != '#';

    assertThat(ZanzibarIds.encode("jane.doe@example.com", noColon))
        .isEqualTo("jane.doe@example.com");
    assertThat(ZanzibarIds.encode("urn:a:b", noColon)).isEqualTo("urn=3Aa=3Ab");
    assertThat(ZanzibarIds.encode("1234567", SPICEDB)).isEqualTo("1234567");
  }

  @Test
  void escapesTheEscapeCharacter() {
    assertThat(ZanzibarIds.encode("a=b", c -> true)).isEqualTo("a=3Db");
    assertThat(ZanzibarIds.decode(ZanzibarIds.encode("=3A", c -> true))).isEqualTo("=3A");
  }

  @Test
  void leavesTheWildcardUnchanged() {
    assertThat(ZanzibarIds.encode("*", SPICEDB)).isEqualTo("*");
    assertThat(ZanzibarIds.decode("*")).isEqualTo("*");
  }
}
