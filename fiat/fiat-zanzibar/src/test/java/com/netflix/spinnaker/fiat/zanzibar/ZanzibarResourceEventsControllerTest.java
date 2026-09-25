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

package com.netflix.spinnaker.fiat.zanzibar;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import com.netflix.spinnaker.kork.zanzibar.ZanzibarStore;
import com.netflix.spinnaker.kork.zanzibar.ingest.InProcessReconcileLock;
import com.netflix.spinnaker.kork.zanzibar.ingest.ReconcileLock;
import com.netflix.spinnaker.kork.zanzibar.ingest.Reconciler;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

/** Verifies the bootstrap endpoint's rate check, its result, and refusal while a run is going. */
class ZanzibarResourceEventsControllerTest {

  private final ZanzibarStore store = mock(ZanzibarStore.class);

  @Test
  void bootstrapReturnsWhatChanged() {
    var response = controller(new InProcessReconcileLock()).bootstrap(1000);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(response.getBody())
        .containsEntry("resourceTuplesChanged", 0L)
        .containsEntry("serviceAccountTuplesChanged", 0L)
        .containsKey("durationMs");
  }

  @Test
  void bootstrapRejectsARateBelowOne() {
    var controller = controller(new InProcessReconcileLock());

    assertThatThrownBy(() -> controller.bootstrap(0)).isInstanceOf(IllegalArgumentException.class);
    verifyNoInteractions(store);
  }

  @Test
  void bootstrapReturns409WhileOneIsRunning() throws Exception {
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    ReconcileLock lock =
        (key, action) -> {
          entered.countDown();
          await(release);
          action.run();
        };
    var controller = controller(lock);
    var executor = Executors.newSingleThreadExecutor();
    try {
      var first = executor.submit(() -> controller.bootstrap(1000));
      assertThat(entered.await(10, SECONDS)).isTrue();

      assertThat(controller.bootstrap(1000).getStatusCode()).isEqualTo(HttpStatus.CONFLICT);

      release.countDown();
      assertThat(first.get(10, SECONDS).getStatusCode()).isEqualTo(HttpStatus.OK);
      assertThat(controller.bootstrap(1000).getStatusCode())
          .as("runs again once the first finishes")
          .isEqualTo(HttpStatus.OK);
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  private ZanzibarResourceEventsController controller(ReconcileLock lock) {
    var bootstrap =
        new ZanzibarBootstrap(
            store,
            lock,
            List.of(),
            new ServiceAccountMembershipReconciler(null, new Reconciler(store), lock));
    return new ZanzibarResourceEventsController(null, null, null, bootstrap);
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
