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

import com.netflix.spinnaker.security.Authorization;
import java.util.Locale;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * A single Zanzibar-style relationship (tuple): {@code
 * <objectType>:<objectId>#<relation>@<subject>}. The core write primitive both engine adapters
 * translate. Subject refs are {@code "user:alice"}, {@code "group:eng"}, or the wildcard {@code
 * "user:*"}.
 */
@Getter
@EqualsAndHashCode
public final class ZanzibarRelationship {

  private final String objectType;
  private final String objectId;
  private final String relation;
  private final String subjectRef;

  private ZanzibarRelationship(
      String objectType, String objectId, String relation, String subjectRef) {
    this.objectType = objectType;
    this.objectId = objectId;
    this.relation = relation;
    this.subjectRef = subjectRef;
  }

  /** General factory, used when reconstructing a tuple read back from the store. */
  public static ZanzibarRelationship of(
      String objectType, String objectId, String relation, String subjectRef) {
    return new ZanzibarRelationship(objectType, objectId, relation, subjectRef);
  }

  /** {@code group:<groupId>#member@<memberRef>} — group membership (memberRef may be a group). */
  public static ZanzibarRelationship member(String groupId, String memberRef) {
    return of("group", groupId, "member", memberRef);
  }

  /** {@code <type>:<name>#<action>@<subjectRef>} — an ACL grant on a resource. */
  public static ZanzibarRelationship acl(
      String type, String name, Authorization action, String subjectRef) {
    return of(type, name, action.name().toLowerCase(Locale.ROOT), subjectRef);
  }
}
