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

import static com.netflix.spinnaker.kork.zanzibar.TestTypes.APPLICATION;
import static com.netflix.spinnaker.security.Authorization.READ;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.netflix.spinnaker.kork.zanzibar.consistency.ConsistencySupport;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Runs without Docker: the decision/enumeration cache serves repeats and invalidates on writes. */
class CachingZanzibarStoreTest {

  private static CachingZanzibarStore caching(FakeZanzibarStore delegate) {
    return new CachingZanzibarStore(delegate, Duration.ofMinutes(1), 10_000);
  }

  @Test
  void servesRepeatedChecksFromCache() {
    var delegate = new FakeZanzibarStore(Map.of());
    var store = caching(delegate);

    store.check("alice", APPLICATION, "foo", READ);
    store.check("alice", APPLICATION, "foo", READ);
    assertThat(delegate.checkCalls).isEqualTo(1); // second served from cache

    store.check("bob", APPLICATION, "foo", READ);
    assertThat(delegate.checkCalls).isEqualTo(2); // distinct key hits the store
  }

  @Test
  void cachesAdminResolution() {
    var delegate = new FakeZanzibarStore(Map.of("admins", Set.of("root")));
    var store = caching(delegate);

    assertThat(store.isMember("root", "admins")).isTrue();
    assertThat(store.isMember("root", "admins")).isTrue();
    assertThat(delegate.isMemberCalls).isEqualTo(1);
  }

  @Test
  void cachesEnumeration() {
    var delegate = new FakeZanzibarStore(Map.of());
    var store = caching(delegate);

    store.lookupResources("alice", APPLICATION, READ);
    store.lookupResources("alice", APPLICATION, READ);
    assertThat(delegate.lookupCalls).isEqualTo(1);
  }

  @Test
  void consistencyIsTheDelegates() {
    var delegate = mock(ConsistencySupport.class);
    when(delegate.consistency()).thenReturn(Optional.of(delegate));

    var store = new CachingZanzibarStore(delegate, Duration.ofMinutes(1), 10_000);

    assertThat(store.consistency()).containsSame(delegate);
  }

  @Test
  void writeInvalidatesCache() {
    var delegate = new FakeZanzibarStore(Map.of());
    var store = caching(delegate);

    store.check("alice", APPLICATION, "foo", READ);
    store.write(List.of(ZanzibarRelationship.acl(APPLICATION, "foo", READ, "group:eng")));
    store.check("alice", APPLICATION, "foo", READ);

    assertThat(delegate.checkCalls).isEqualTo(2); // re-evaluated after the write evicted the entry
  }
}
