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

package com.netflix.spinnaker.fiat;

import static org.assertj.core.api.Assertions.assertThat;

import com.netflix.spinnaker.fiat.controllers.RolesController;
import com.netflix.spinnaker.fiat.permissions.PermissionsRepository;
import com.netflix.spinnaker.fiat.permissions.RedisPermissionsRepository;
import com.netflix.spinnaker.fiat.zanzibar.ZanzibarPermissionsRepository;
import com.netflix.spinnaker.fiat.zanzibar.ZanzibarResourceEventsController;
import com.netflix.spinnaker.kork.zanzibar.ZanzibarSchema;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/** Fiat starts in Zanzibar mode against a real SpiceDB. Requires Docker. */
@SpringBootTest(classes = {Main.class})
@TestPropertySource(
    properties = {
      "spring.config.location=classpath:fiat-test.yml",
      "fiat.zanzibar.enabled=true",
      "permissions-repository.redis.enabled=false"
    })
class ZanzibarMainTest {

  private static final String SPICEDB_KEY = "zanzibar-main-key";

  static final GenericContainer<?> SPICEDB =
      new GenericContainer<>("authzed/spicedb:latest")
          .withCommand("serve", "--grpc-preshared-key", SPICEDB_KEY)
          .withExposedPorts(50051)
          .waitingFor(Wait.forListeningPort());

  static {
    SPICEDB.start();
  }

  @DynamicPropertySource
  static void spicedb(DynamicPropertyRegistry registry) {
    registry.add("fiat.zanzibar.spicedb.host", SPICEDB::getHost);
    registry.add("fiat.zanzibar.spicedb.port", () -> SPICEDB.getMappedPort(50051));
    registry.add("fiat.zanzibar.spicedb.preshared-key", () -> SPICEDB_KEY);
  }

  @Autowired ApplicationContext context;
  @Autowired PermissionsRepository permissionsRepository;

  @Test
  void startsWithTheStoreBackedRepository() {
    assertThat(permissionsRepository).isInstanceOf(ZanzibarPermissionsRepository.class);
    assertThat(context.getBeansOfType(RedisPermissionsRepository.class)).isEmpty();
    assertThat(context.getBeansOfType(ZanzibarResourceEventsController.class)).hasSize(1);
    assertThat(context.getBeansOfType(RolesController.class)).hasSize(1);
    assertThat(context.getBean(ZanzibarSchema.class).getResourceTypes())
        .containsExactly("account", "application", "build_service");
    assertThat(permissionsRepository.isEmpty()).isTrue();
  }
}
