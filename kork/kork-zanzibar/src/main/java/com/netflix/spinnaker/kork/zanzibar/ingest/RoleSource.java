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

package com.netflix.spinnaker.kork.zanzibar.ingest;

/**
 * Streams group-membership edges into the ingestion pipeline — the role side of authorization. An
 * implementation adapts a role provider (LDAP, Google Groups, GitHub teams, a directory dump) or a
 * generator. Emissions must be unique; {@code memberRef} is {@code "user:<id>"} or {@code
 * "group:<id>"}.
 */
@FunctionalInterface
public interface RoleSource {

  void forEach(MembershipConsumer consumer);

  @FunctionalInterface
  interface MembershipConsumer {
    void accept(String groupId, String memberRef);
  }
}
