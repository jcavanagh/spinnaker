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

import com.netflix.spinnaker.kork.zanzibar.consistency.ConsistencySupport;
import com.netflix.spinnaker.security.Authorization;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Engine-agnostic relationship store — the decision-layer seam a store-backed {@code
 * PolicyDecisionPoint} sits on. The store owns two graphs: group membership (including nested
 * groups) and per-resource ACLs. Callers ask {@link #check} by <em>identity</em>; the store expands
 * the caller's roles internally, so a caller with 10k+ roles never enumerates them.
 *
 * <p>Subject/member references are {@code "<type>:<id>"}: {@code "user:alice"}, {@code
 * "group:eng"}, or the wildcard {@code "user:*"} (world-accessible). A group reference grants to
 * its members.
 *
 * <p>{@link #write}/{@link #delete} are the batch primitives (adapters chunk to their engine
 * limits); the single-tuple helpers delegate to them. Two adapters implement this ({@code
 * SpiceDbZanzibarStore}, {@code OpenFgaZanzibarStore}), selectable behind one interface.
 */
public interface ZanzibarStore extends AutoCloseable {

  /** Stable id used for logging and engine selection. */
  String providerId();

  /** Install the Spinnaker authorization schema/model into the store. */
  void applySchema();

  /** Upsert a batch of relationships. */
  void write(Collection<ZanzibarRelationship> tuples);

  /** Delete a batch of relationships. */
  void delete(Collection<ZanzibarRelationship> tuples);

  /** All relationships currently stored on the given object, for reconciliation diffing. */
  Set<ZanzibarRelationship> read(String objectType, String objectId);

  /**
   * Distinct object ids of the given type that have at least one relationship (for orphan sweeps).
   */
  Set<String> objectIds(String objectType);

  /** Whether the store holds no relationships at all. */
  boolean isEmpty();

  /**
   * Apply {@code writes} and {@code deletes} (deletes first). Small diffs are a single engine
   * transaction; larger diffs are chunked to the engine's per-transaction cap and so span several.
   * Safe because reconciliation is idempotent: the next reconcile of an object applies whatever a
   * partial application left out.
   */
  void apply(Collection<ZanzibarRelationship> writes, Collection<ZanzibarRelationship> deletes);

  /** The most relationships one write request carries. */
  default int writeBatchSize() {
    return Integer.MAX_VALUE;
  }

  /** The engine store underneath, when its engine has consistency tokens. */
  default Optional<ConsistencySupport> consistency() {
    return Optional.empty();
  }

  /** Decide whether {@code userId} may perform {@code action} on the named resource. */
  boolean check(String userId, String type, String resourceName, Authorization action);

  /**
   * Whether {@code userId} is a (transitive) member of {@code groupId}. Used for admin resolution.
   */
  boolean isMember(String userId, String groupId);

  /**
   * The groups {@code userId} is a <em>direct</em> member of — for per-user membership
   * reconciliation (diffing a user's stored memberships against what the directory currently
   * reports).
   */
  Set<String> groupsOf(String userId);

  /**
   * The names of {@code type} resources the user may perform {@code action} on — the enumeration
   * behind UI list/filter (Fiat's {@code UserPermission.View}), computed live rather than
   * materialized.
   */
  Set<String> lookupResources(String userId, String type, Authorization action);

  /**
   * Narrows {@link AutoCloseable#close()} — adapters release only local resources, no checked
   * throw.
   */
  @Override
  void close();

  default void addMember(String groupId, String memberRef) {
    write(List.of(ZanzibarRelationship.member(groupId, memberRef)));
  }

  default void removeMember(String groupId, String memberRef) {
    delete(List.of(ZanzibarRelationship.member(groupId, memberRef)));
  }

  default void addAcl(String type, String name, Authorization action, String subjectRef) {
    write(List.of(ZanzibarRelationship.acl(type, name, action, subjectRef)));
  }

  default void removeAcl(String type, String name, Authorization action, String subjectRef) {
    delete(List.of(ZanzibarRelationship.acl(type, name, action, subjectRef)));
  }
}
