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

package com.netflix.spinnaker.kork.zanzibar.openfga;

import static org.assertj.core.api.Assertions.assertThat;

import com.netflix.spinnaker.kork.zanzibar.ZanzibarSchema;
import dev.openfga.sdk.api.client.OpenFgaClient;
import dev.openfga.sdk.api.configuration.ClientConfiguration;
import dev.openfga.sdk.api.configuration.ClientListStoresOptions;
import dev.openfga.sdk.api.configuration.ClientReadAuthorizationModelsOptions;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Verifies the store reuses its authorization model across restarts. Requires Docker. */
@Testcontainers
class OpenFgaZanzibarStoreTest {

  private static final String STORE_NAME = "reuse";

  @Container
  static final GenericContainer<?> OPENFGA =
      new GenericContainer<>("openfga/openfga:latest")
          .withCommand("run", "--http-addr", "0.0.0.0:9190")
          .withExposedPorts(9190)
          .waitingFor(Wait.forHttp("/healthz").forPort(9190).forStatusCode(200));

  @Test
  void restartsReuseTheModelUntilTheSchemaChanges() throws Exception {
    var url = "http://" + OPENFGA.getHost() + ":" + OPENFGA.getMappedPort(9190);
    var schema = new ZanzibarSchema(List.of("application"));
    for (var restart = 0; restart < 2; restart++) {
      var store = new OpenFgaZanzibarStore(schema, url, STORE_NAME);
      store.applySchema();
      store.close();
    }
    assertThat(modelCount(url)).as("restarts reuse the model").isEqualTo(1);

    var changed =
        new OpenFgaZanzibarStore(
            new ZanzibarSchema(List.of("application", "account")), url, STORE_NAME);
    changed.applySchema();
    changed.close();
    assertThat(modelCount(url)).as("a changed schema writes a new version").isEqualTo(2);
  }

  /** Authorization models held by the store named {@link #STORE_NAME}. */
  private static int modelCount(String url) throws Exception {
    var client = new OpenFgaClient(new ClientConfiguration().apiUrl(url));
    var store =
        client.listStores(new ClientListStoresOptions().name(STORE_NAME)).get().getStores().stream()
            .filter(candidate -> STORE_NAME.equals(candidate.getName()))
            .findFirst()
            .orElseThrow();
    client.setStoreId(store.getId());
    return client
        .readAuthorizationModels(new ClientReadAuthorizationModelsOptions().pageSize(10))
        .get()
        .getAuthorizationModels()
        .size();
  }
}
