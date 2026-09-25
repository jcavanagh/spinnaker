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

package com.netflix.spinnaker.fiat.testing;

import com.netflix.spinnaker.fiat.model.Authorization;
import com.netflix.spinnaker.fiat.model.resources.Permissions;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Deterministic, production-shaped authorization data shared by the Fiat baseline and store
 * benchmarks, so both measure identical users, roles, resource ACLs, and service accounts. Group
 * ids are numeric. Every name uses only characters all engines accept.
 */
public final class ProductionShapedFixtures {

  private ProductionShapedFixtures() {}

  public static String userId(int user) {
    return "user-" + user;
  }

  public static String groupId(int group) {
    return String.valueOf(1_000_000 + group);
  }

  /** A group outside the {@code groups} pool, so only users granted it explicitly are admins. */
  public static String adminGroupId(int groups) {
    return groupId(groups);
  }

  public static String accountName(int i) {
    return "account-" + i;
  }

  public static String applicationName(int i) {
    return "app-" + i;
  }

  public static String serviceAccountName(int i) {
    return "svc-" + i + "-managed-service-account";
  }

  /** {@code rolesPerUser} distinct group ids drawn from a pool of {@code groups}. */
  public static Set<String> rolesOf(int user, int rolesPerUser, int groups) {
    if (rolesPerUser > groups) {
      throw new IllegalArgumentException(
          "rolesPerUser (" + rolesPerUser + ") exceeds groups (" + groups + ")");
    }
    var rnd = new Random(31L * user + 1);
    var roles = new LinkedHashSet<String>(rolesPerUser * 2);
    while (roles.size() < rolesPerUser) {
      roles.add(groupId(rnd.nextInt(groups)));
    }
    return roles;
  }

  /**
   * A resource ACL: {@code rolesPerResource} READ groups plus one WRITE group, or unrestricted
   * (world-accessible) for {@code unrestrictedFraction} of resources.
   */
  public static Permissions permissionsOf(
      String resourceName, int rolesPerResource, int groups, double unrestrictedFraction) {
    var rnd = new Random(resourceName.hashCode());
    if (rnd.nextDouble() < unrestrictedFraction) {
      return Permissions.EMPTY;
    }
    var builder = new Permissions.Builder();
    for (int k = 0; k < rolesPerResource; k++) {
      builder.add(Authorization.READ, groupId(rnd.nextInt(groups)));
    }
    builder.add(Authorization.WRITE, groupId(rnd.nextInt(groups)));
    return builder.build();
  }

  /** Two groups; a user needs both to use the service account (Fiat's default AND mode). */
  public static List<String> serviceAccountMemberOf(int i, int groups) {
    var rnd = new Random(7L * i + 3);
    return List.of(groupId(rnd.nextInt(groups)), groupId(rnd.nextInt(groups)));
  }
}
