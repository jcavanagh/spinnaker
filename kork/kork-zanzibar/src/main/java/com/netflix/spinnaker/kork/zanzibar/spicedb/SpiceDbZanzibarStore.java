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

package com.netflix.spinnaker.kork.zanzibar.spicedb;

import com.authzed.api.v1.CheckPermissionRequest;
import com.authzed.api.v1.CheckPermissionResponse;
import com.authzed.api.v1.Consistency;
import com.authzed.api.v1.LookupResourcesRequest;
import com.authzed.api.v1.ObjectReference;
import com.authzed.api.v1.PermissionsServiceGrpc;
import com.authzed.api.v1.ReadRelationshipsRequest;
import com.authzed.api.v1.Relationship;
import com.authzed.api.v1.RelationshipFilter;
import com.authzed.api.v1.RelationshipUpdate;
import com.authzed.api.v1.SchemaServiceGrpc;
import com.authzed.api.v1.SubjectFilter;
import com.authzed.api.v1.SubjectReference;
import com.authzed.api.v1.WatchRequest;
import com.authzed.api.v1.WatchResponse;
import com.authzed.api.v1.WatchServiceGrpc;
import com.authzed.api.v1.WriteRelationshipsRequest;
import com.authzed.api.v1.WriteSchemaRequest;
import com.authzed.api.v1.ZedToken;
import com.authzed.grpcutil.BearerToken;
import com.netflix.spinnaker.kork.zanzibar.ZanzibarIds;
import com.netflix.spinnaker.kork.zanzibar.ZanzibarRelationship;
import com.netflix.spinnaker.kork.zanzibar.ZanzibarSchema;
import com.netflix.spinnaker.kork.zanzibar.consistency.ConsistencyProperties;
import com.netflix.spinnaker.kork.zanzibar.consistency.ConsistencySupport;
import com.netflix.spinnaker.kork.zanzibar.consistency.ConsistencyToken;
import com.netflix.spinnaker.kork.zanzibar.consistency.ConsistencyTokens;
import com.netflix.spinnaker.security.Authorization;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.IntPredicate;
import javax.annotation.Nullable;

/** SpiceDB (AuthZed) adapter over the v1 gRPC API. */
public class SpiceDbZanzibarStore implements ConsistencySupport {

  /** SpiceDB caps WriteRelationships updates per request; chunk below the server limit. */
  private static final int MAX_BATCH = 500;

  /** SpiceDB's object id charset; {@code =} is reserved as the escape. */
  private static final IntPredicate ID_CHARS =
      c ->
          (c >= 'a' && c <= 'z')
              || (c >= 'A' && c <= 'Z')
              || (c >= '0' && c <= '9')
              || c == '/'
              || c == '_'
              || c == '|'
              || c == '-'
              || c == '+';

  private static final Consistency FULLY_CONSISTENT =
      Consistency.newBuilder().setFullyConsistent(true).build();

  private final ZanzibarSchema zanzibarSchema;
  private final String host;
  private final int port;
  private final ConsistencyTokens tokens;
  private final ManagedChannel channel;
  private final SchemaServiceGrpc.SchemaServiceBlockingStub schema;
  private final PermissionsServiceGrpc.PermissionsServiceBlockingStub permissions;
  private final WatchServiceGrpc.WatchServiceStub watch;

  public SpiceDbZanzibarStore(
      ZanzibarSchema zanzibarSchema, String host, int port, String presharedKey) {
    this(zanzibarSchema, host, port, presharedKey, new ConsistencyProperties());
  }

  public SpiceDbZanzibarStore(
      ZanzibarSchema zanzibarSchema,
      String host,
      int port,
      String presharedKey,
      ConsistencyProperties consistency) {
    this.zanzibarSchema = zanzibarSchema;
    this.host = host;
    this.port = port;
    this.tokens = new ConsistencyTokens(consistency);
    this.channel =
        ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .maxInboundMessageSize(16 << 20)
            .build();
    var token = new BearerToken(presharedKey);
    this.schema = SchemaServiceGrpc.newBlockingStub(channel).withCallCredentials(token);
    this.permissions = PermissionsServiceGrpc.newBlockingStub(channel).withCallCredentials(token);
    this.watch = WatchServiceGrpc.newStub(channel).withCallCredentials(token);
  }

  @Override
  public String providerId() {
    return "spicedb";
  }

  @Override
  public int writeBatchSize() {
    return MAX_BATCH;
  }

  @Override
  public Optional<ConsistencySupport> consistency() {
    return Optional.of(this);
  }

  @Override
  public ConsistencyTokens tokens() {
    return tokens;
  }

  @Override
  public String consistencyKey() {
    return "spicedb:" + host + ":" + port;
  }

  @Override
  public Runnable watch(
      @Nullable ConsistencyToken from,
      Consumer<ConsistencyToken> onToken,
      Consumer<Throwable> onEnd) {
    var request = WatchRequest.newBuilder();
    if (from != null) {
      request.setOptionalStartCursor(ZedToken.newBuilder().setToken(from.value()));
    }
    var call = new AtomicReference<ClientCallStreamObserver<WatchRequest>>();
    watch.watch(
        request.build(),
        new ClientResponseObserver<WatchRequest, WatchResponse>() {
          @Override
          public void beforeStart(ClientCallStreamObserver<WatchRequest> stream) {
            call.set(stream);
          }

          @Override
          public void onNext(WatchResponse response) {
            if (response.hasChangesThrough()) {
              onToken.accept(token(response.getChangesThrough()));
            }
          }

          @Override
          public void onError(Throwable error) {
            onEnd.accept(error);
          }

          @Override
          public void onCompleted() {
            onEnd.accept(null);
          }
        });
    return () -> call.get().cancel("Stopped watching", null);
  }

  /** SpiceDB's tokens are opaque, so they're ordered by when they arrived. */
  private static ConsistencyToken token(ZedToken token) {
    return ConsistencyToken.of(token.getToken(), System.currentTimeMillis());
  }

  /** At least as fresh as the newest token, else fully consistent. */
  private Consistency readConsistency() {
    return tokens
        .current()
        .map(
            token ->
                Consistency.newBuilder()
                    .setAtLeastAsFresh(ZedToken.newBuilder().setToken(token.value()))
                    .build())
        .orElse(FULLY_CONSISTENT);
  }

  @Override
  public void applySchema() {
    schema.writeSchema(
        WriteSchemaRequest.newBuilder().setSchema(SpiceDbSchema.render(zanzibarSchema)).build());
  }

  @Override
  public void write(Collection<ZanzibarRelationship> tuples) {
    batchUpdate(tuples, RelationshipUpdate.Operation.OPERATION_TOUCH);
  }

  @Override
  public void delete(Collection<ZanzibarRelationship> tuples) {
    batchUpdate(tuples, RelationshipUpdate.Operation.OPERATION_DELETE);
  }

  private void batchUpdate(
      Collection<ZanzibarRelationship> tuples, RelationshipUpdate.Operation op) {
    var chunk = new java.util.ArrayList<ZanzibarRelationship>(MAX_BATCH);
    for (var tuple : tuples) {
      chunk.add(tuple);
      if (chunk.size() == MAX_BATCH) {
        flush(chunk, op);
        chunk.clear();
      }
    }
    if (!chunk.isEmpty()) {
      flush(chunk, op);
    }
  }

  private void flush(List<ZanzibarRelationship> chunk, RelationshipUpdate.Operation op) {
    var request = WriteRelationshipsRequest.newBuilder();
    for (var tuple : chunk) {
      request.addUpdates(update(op, tuple));
    }
    tokens.observe(token(permissions.writeRelationships(request.build()).getWrittenAt()));
  }

  private static RelationshipUpdate update(
      RelationshipUpdate.Operation op, ZanzibarRelationship tuple) {
    return RelationshipUpdate.newBuilder()
        .setOperation(op)
        .setRelationship(
            Relationship.newBuilder()
                .setResource(object(tuple.getObjectType(), tuple.getObjectId()))
                .setRelation(tuple.getRelation())
                .setSubject(subject(tuple.getSubjectRef()))
                .build())
        .build();
  }

  @Override
  public Set<ZanzibarRelationship> read(String objectType, String objectId) {
    var request =
        ReadRelationshipsRequest.newBuilder()
            .setConsistency(Consistency.newBuilder().setFullyConsistent(true).build())
            .setRelationshipFilter(
                RelationshipFilter.newBuilder()
                    .setResourceType(objectType)
                    .setOptionalResourceId(encode(objectId))
                    .build())
            .build();
    var responses = permissions.readRelationships(request);
    var result = new LinkedHashSet<ZanzibarRelationship>();
    while (responses.hasNext()) {
      result.add(relationship(responses.next().getRelationship()));
    }
    return result;
  }

  @Override
  public Set<String> objectIds(String objectType) {
    var request =
        ReadRelationshipsRequest.newBuilder()
            .setConsistency(Consistency.newBuilder().setFullyConsistent(true).build())
            .setRelationshipFilter(
                RelationshipFilter.newBuilder().setResourceType(objectType).build())
            .build();
    var responses = permissions.readRelationships(request);
    var ids = new LinkedHashSet<String>();
    while (responses.hasNext()) {
      ids.add(ZanzibarIds.decode(responses.next().getRelationship().getResource().getObjectId()));
    }
    return ids;
  }

  @Override
  public boolean isEmpty() {
    var types = new ArrayList<String>(List.of("group"));
    types.addAll(zanzibarSchema.getResourceTypes());
    for (var type : types) {
      // A cached revision can predate the schema, which fails the read.
      var request =
          ReadRelationshipsRequest.newBuilder()
              .setConsistency(Consistency.newBuilder().setFullyConsistent(true).build())
              .setRelationshipFilter(RelationshipFilter.newBuilder().setResourceType(type).build())
              .setOptionalLimit(1)
              .build();
      var responses = permissions.readRelationships(request);
      var found = responses.hasNext();
      while (responses.hasNext()) {
        responses.next();
      }
      if (found) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void apply(
      Collection<ZanzibarRelationship> writes, Collection<ZanzibarRelationship> deletes) {
    // Chunked to SpiceDB's per-request cap (deletes first); a large diff spans several requests.
    // Not one atomic transaction; the next reconcile of an object applies whatever is left.
    batchUpdate(deletes, RelationshipUpdate.Operation.OPERATION_DELETE);
    batchUpdate(writes, RelationshipUpdate.Operation.OPERATION_TOUCH);
  }

  @Override
  public boolean check(String userId, String type, String name, Authorization action) {
    return hasPermission(type, name, relation(action), subject("user:" + userId));
  }

  @Override
  public boolean isMember(String userId, String groupId) {
    return hasPermission("group", groupId, "member", subject("user:" + userId));
  }

  @Override
  public Set<String> groupsOf(String userId) {
    var request =
        ReadRelationshipsRequest.newBuilder()
            .setConsistency(Consistency.newBuilder().setFullyConsistent(true).build())
            .setRelationshipFilter(
                RelationshipFilter.newBuilder()
                    .setResourceType("group")
                    .setOptionalRelation("member")
                    .setOptionalSubjectFilter(
                        SubjectFilter.newBuilder()
                            .setSubjectType("user")
                            .setOptionalSubjectId(encode(userId))
                            .build())
                    .build())
            .build();
    var responses = permissions.readRelationships(request);
    var groups = new LinkedHashSet<String>();
    while (responses.hasNext()) {
      groups.add(
          ZanzibarIds.decode(responses.next().getRelationship().getResource().getObjectId()));
    }
    return groups;
  }

  @Override
  public Set<String> lookupResources(String userId, String type, Authorization action) {
    var request =
        LookupResourcesRequest.newBuilder()
            .setConsistency(readConsistency())
            .setResourceObjectType(type)
            .setPermission(relation(action))
            .setSubject(subject("user:" + userId))
            .build();
    var responses = permissions.lookupResources(request);
    var ids = new LinkedHashSet<String>();
    while (responses.hasNext()) {
      ids.add(ZanzibarIds.decode(responses.next().getResourceObjectId()));
    }
    return ids;
  }

  private boolean hasPermission(
      String objectType, String objectId, String permission, SubjectReference subject) {
    var response =
        permissions.checkPermission(
            CheckPermissionRequest.newBuilder()
                .setConsistency(readConsistency())
                .setResource(object(objectType, objectId))
                .setPermission(permission)
                .setSubject(subject)
                .build());
    return response.getPermissionship()
        == CheckPermissionResponse.Permissionship.PERMISSIONSHIP_HAS_PERMISSION;
  }

  private static ObjectReference object(String type, String id) {
    return ObjectReference.newBuilder().setObjectType(type).setObjectId(encode(id)).build();
  }

  private static String encode(String id) {
    return ZanzibarIds.encode(id, ID_CHARS);
  }

  /** A stored relationship in engine-agnostic form, with ids decoded. */
  private static ZanzibarRelationship relationship(Relationship relationship) {
    var subject = relationship.getSubject().getObject();
    return ZanzibarRelationship.of(
        relationship.getResource().getObjectType(),
        ZanzibarIds.decode(relationship.getResource().getObjectId()),
        relationship.getRelation(),
        subject.getObjectType() + ":" + ZanzibarIds.decode(subject.getObjectId()));
  }

  /**
   * Parse {@code "<type>:<id>"}; a {@code group} reference resolves to the {@code #member} userset.
   */
  private static SubjectReference subject(String ref) {
    var parts = ref.split(":", 2);
    var builder = SubjectReference.newBuilder().setObject(object(parts[0], parts[1]));
    if ("group".equals(parts[0])) {
      builder.setOptionalRelation("member");
    }
    return builder.build();
  }

  private static String relation(Authorization action) {
    return action.name().toLowerCase(Locale.ROOT);
  }

  @Override
  public void close() {
    channel.shutdownNow();
    try {
      channel.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
