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

import static com.netflix.spinnaker.security.Authorization.READ;
import static com.netflix.spinnaker.security.Authorization.WRITE;

import com.netflix.spinnaker.kork.zanzibar.ingest.ResourceSource;
import com.netflix.spinnaker.kork.zanzibar.ingest.RoleSource;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/** Fixtures (correctness) and generators (scale) for the ingestion pipelines and access tests. */
public final class SyntheticData {

  // Fixture graph. eng-sub nests into eng; admins drives admin login.
  public static final String ENG = "eng";
  public static final String ENG_SUB = "eng-sub";
  public static final String OPS = "ops";
  public static final String ADMINS = "admins";

  public static final String ALICE = "alice"; // direct eng member
  public static final String BOB = "bob"; // eng via nested eng-sub
  public static final String CAROL = "carol"; // no memberships
  public static final String DAVE = "dave"; // ops member
  public static final String ROOT = "root"; // admins member

  private SyntheticData() {}

  /**
   * Name of the read/write-restricted instance of a resource type ({@code eng} reads, {@code ops}
   * writes).
   */
  public static String secure(String type) {
    return type + "-secure";
  }

  /** Name of the world-accessible (unrestricted) instance of a resource type. */
  public static String open(String type) {
    return type + "-open";
  }

  public static RoleSource fixtureRoles() {
    return consumer -> {
      consumer.accept(ENG, "group:" + ENG_SUB);
      consumer.accept(ENG, "user:" + ALICE);
      consumer.accept(ENG_SUB, "user:" + BOB);
      consumer.accept(OPS, "user:" + DAVE);
      consumer.accept(ADMINS, "user:" + ROOT);
    };
  }

  /** One restricted and one unrestricted instance of every resource type Fiat supports. */
  public static ResourceSource fixtureResources() {
    return consumer -> {
      for (var type : TestTypes.SCHEMA.getResourceTypes()) {
        var restricted = Map.of(READ, Set.of(ENG), WRITE, Set.of(OPS));
        consumer.accept(type, secure(type), restricted);
        consumer.accept(type, open(type), Map.of());
      }
    };
  }

  /**
   * A nested group graph plus user memberships. Groups form a tree ({@code g_i} is a member of
   * {@code g_(i-1)/fanout}), so authorizing against a top group requires transitive expansion.
   */
  /**
   * Per-user role memberships drawn from a catalog of {@code numRoles} roles — the shape production
   * actually stores: login writes one flat {@code member(role, user)} per role the user holds; no
   * group hierarchy is pre-filled and no nesting is synced, so only assigned roles exist. Each user
   * gets {@code rolesPerUser} distinct random roles (capped at {@code numRoles}).
   */
  public static RoleSource roleMemberships(
      int numRoles, int numUsers, int rolesPerUser, long seed) {
    if (rolesPerUser > numRoles) {
      throw new IllegalArgumentException(
          "rolesPerUser " + rolesPerUser + " exceeds the role catalog size " + numRoles);
    }
    return consumer -> {
      var rnd = new Random(seed);
      for (int u = 0; u < numUsers; u++) {
        var picked = new HashSet<Integer>();
        while (picked.size() < rolesPerUser) {
          picked.add(rnd.nextInt(numRoles));
        }
        for (var role : picked) {
          consumer.accept("g" + role, "user:u" + u);
        }
      }
    };
  }

  /**
   * {@code perType} resources of each type; a fraction are unrestricted, the rest grant random
   * groups.
   */
  public static ResourceSource resources(
      int perType, double unrestrictedRatio, int numGroups, int rolesPerResource, long seed) {
    return consumer -> {
      var rnd = new Random(seed);
      for (var type : TestTypes.SCHEMA.getResourceTypes()) {
        for (int i = 0; i < perType; i++) {
          var name = type + "-" + i;
          if (rnd.nextDouble() < unrestrictedRatio) {
            consumer.accept(type, name, Map.of());
          } else {
            var readers = new HashSet<String>();
            for (int k = 0; k < rolesPerResource; k++) {
              readers.add("g" + rnd.nextInt(numGroups));
            }
            consumer.accept(
                type, name, Map.of(READ, readers, WRITE, Set.of("g" + rnd.nextInt(numGroups))));
          }
        }
      }
    };
  }
}
