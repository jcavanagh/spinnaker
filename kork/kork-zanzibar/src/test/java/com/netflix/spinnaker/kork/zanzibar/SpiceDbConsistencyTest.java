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
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.netflix.spinnaker.kork.zanzibar.consistency.ConsistencyProperties;
import com.netflix.spinnaker.kork.zanzibar.consistency.ConsistencyProperties.Strategy;
import com.netflix.spinnaker.kork.zanzibar.consistency.ConsistencyToken;
import com.netflix.spinnaker.kork.zanzibar.spicedb.SpiceDbZanzibarStore;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * SpiceDB consistency tokens: writes record them, checks at a token see the write, and the watch
 * streams one per change. Requires Docker.
 */
@Testcontainers
class SpiceDbConsistencyTest {

  private static final String KEY = "consistency-test-key";

  @Container
  static final GenericContainer<?> SPICEDB =
      new GenericContainer<>("authzed/spicedb:latest")
          .withCommand("serve", "--grpc-preshared-key", KEY)
          .withExposedPorts(50051)
          .waitingFor(Wait.forListeningPort());

  @Test
  void aWriteRecordsItsTokenAndACheckSeesIt() {
    try (var store = store(Strategy.LOCAL)) {
      assertThat(store.tokens().current()).isEmpty();

      store.addMember("cs-eng", "user:cs-alice");
      store.addAcl(APPLICATION, "cs-app", READ, "group:cs-eng");

      assertThat(store.tokens().current()).isPresent();
      assertThat(store.check("cs-alice", APPLICATION, "cs-app", READ)).isTrue();
    }
  }

  @Test
  void anotherReplicasTokenShowsItsWrite() {
    try (var writer = store(Strategy.LOCAL);
        var reader = store(Strategy.LOCAL)) {
      writer.addMember("cs-ops", "user:cs-bob");
      writer.addAcl(APPLICATION, "cs-ops-app", READ, "group:cs-ops");

      reader.tokens().observe(writer.tokens().current().orElseThrow());

      assertThat(reader.check("cs-bob", APPLICATION, "cs-ops-app", READ)).isTrue();
    }
  }

  @Test
  void withoutTokensChecksStayFullyConsistent() {
    try (var store = store(Strategy.NONE)) {
      store.addMember("cs-dev", "user:cs-carol");
      store.addAcl(APPLICATION, "cs-dev-app", READ, "group:cs-dev");

      assertThat(store.tokens().current()).isEmpty();
      assertThat(store.check("cs-carol", APPLICATION, "cs-dev-app", READ)).isTrue();
    }
  }

  @Test
  void theWatchStreamsATokenForAWrite() throws Exception {
    try (var store = store(Strategy.WATCH)) {
      var tokens = new LinkedBlockingQueue<ConsistencyToken>();
      var ended = new LinkedBlockingQueue<Optional<Throwable>>();
      var stop = store.watch(null, tokens::add, error -> ended.add(Optional.ofNullable(error)));

      // The stream starts asynchronously, so write until it reports a change.
      ConsistencyToken token = null;
      for (int i = 0; i < 20 && token == null; i++) {
        store.addMember("cs-watch", "user:cs-watcher-" + i);
        token = tokens.poll(500, MILLISECONDS);
      }
      assertThat(token).isNotNull();

      stop.run();
      assertThat(ended.poll(5, SECONDS)).as("ended with the cancellation").isPresent();
    }
  }

  @Test
  void theKeyNamesTheServer() {
    try (var store = store(Strategy.LOCAL)) {
      assertThat(store.consistencyKey())
          .isEqualTo("spicedb:" + SPICEDB.getHost() + ":" + SPICEDB.getMappedPort(50051));
    }
  }

  private static SpiceDbZanzibarStore store(Strategy strategy) {
    var consistency = new ConsistencyProperties();
    consistency.setStrategy(strategy);
    var store =
        new SpiceDbZanzibarStore(
            TestTypes.SCHEMA, SPICEDB.getHost(), SPICEDB.getMappedPort(50051), KEY, consistency);
    store.applySchema();
    return store;
  }
}
