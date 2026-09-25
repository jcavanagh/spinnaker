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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.spinnaker.kork.zanzibar.ZanzibarIds;
import com.netflix.spinnaker.kork.zanzibar.ZanzibarRelationship;
import com.netflix.spinnaker.kork.zanzibar.ZanzibarSchema;
import com.netflix.spinnaker.kork.zanzibar.ZanzibarStore;
import com.netflix.spinnaker.security.Authorization;
import dev.openfga.sdk.api.client.OpenFgaClient;
import dev.openfga.sdk.api.client.model.ClientCheckRequest;
import dev.openfga.sdk.api.client.model.ClientListObjectsRequest;
import dev.openfga.sdk.api.client.model.ClientReadRequest;
import dev.openfga.sdk.api.client.model.ClientStreamedListObjectsOptions;
import dev.openfga.sdk.api.client.model.ClientTupleKey;
import dev.openfga.sdk.api.client.model.ClientTupleKeyWithoutCondition;
import dev.openfga.sdk.api.client.model.ClientWriteRequest;
import dev.openfga.sdk.api.configuration.ClientConfiguration;
import dev.openfga.sdk.api.configuration.ClientListStoresOptions;
import dev.openfga.sdk.api.configuration.ClientReadAuthorizationModelsOptions;
import dev.openfga.sdk.api.configuration.ClientReadOptions;
import dev.openfga.sdk.api.configuration.ClientWriteOptions;
import dev.openfga.sdk.api.model.AuthorizationModel;
import dev.openfga.sdk.api.model.CreateStoreRequest;
import dev.openfga.sdk.api.model.Store;
import dev.openfga.sdk.api.model.WriteAuthorizationModelRequest;
import dev.openfga.sdk.api.model.WriteRequestDeletes;
import dev.openfga.sdk.api.model.WriteRequestWrites;
import dev.openfga.sdk.errors.FgaInvalidParameterException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntPredicate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** OpenFGA adapter over the high-level Java SDK (HTTP). */
public class OpenFgaZanzibarStore implements ZanzibarStore {

  private static final Logger log = LogManager.getLogger(OpenFgaZanzibarStore.class);

  /** OpenFGA caps tuples per write transaction; chunk below the server limit. */
  private static final int MAX_BATCH = 100;

  /**
   * Chunk writes issued concurrently. OpenFGA's transactional write is capped at {@link
   * #MAX_BATCH}, so bulk work is many small requests; the SDK is async, so keeping several in
   * flight cuts ingest wall time roughly linearly. Tunable via {@code
   * -Dzanzibar.openfga.writeParallelism}.
   */
  private static final int MAX_PARALLEL =
      Integer.getInteger("zanzibar.openfga.writeParallelism", 16);

  /**
   * OpenFGA parses {@code type:id#relation}; ids may not contain {@code :}, {@code #}, or spaces.
   */
  private static final IntPredicate ID_CHARS = c -> c > 0x20 && c < 0x7F && c != ':' && c != '#';

  private static final ObjectMapper MAPPER =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  /** Default OpenFGA store name; the store is reused by this name across restarts. */
  private static final String DEFAULT_STORE_NAME = "spinnaker";

  /** OpenFGA's default list deadline. */
  private static final Duration DEFAULT_LIST_DEADLINE = Duration.ofSeconds(3);

  /** Time past the list deadline for the server to end a listing's stream. */
  private static final Duration LIST_DEADLINE_MARGIN = Duration.ofSeconds(10);

  private final OpenFgaClient client;
  private final String storeName;

  /** The server's list deadline; a listing that runs this long may be truncated. */
  private final Duration listDeadline;

  private final ZanzibarSchema zanzibarSchema;

  /**
   * Send each chunk as one transaction. Without this the SDK's transaction chunk size defaults to 1
   * — it would fan each write out into one HTTP request per tuple, dominating ingest time.
   *
   * <p>{@code IGNORE} on duplicates/missing makes writes and deletes idempotent: our reconcile is
   * already idempotent, and it lets the SDK's automatic retries be safe (a timed-out write that
   * partially committed won't fail its retry with "already exists") while avoiding the per-tuple
   * existence pre-check that {@code ERROR} forces.
   */
  private final ClientWriteOptions writeOptions =
      new ClientWriteOptions()
          .transactionChunkSize(MAX_BATCH)
          .onDuplicate(WriteRequestWrites.OnDuplicateEnum.IGNORE)
          .onMissing(WriteRequestDeletes.OnMissingEnum.IGNORE);

  public OpenFgaZanzibarStore(ZanzibarSchema zanzibarSchema, String apiUrl) {
    this(zanzibarSchema, apiUrl, null);
  }

  /**
   * @param storeName store to reuse-or-create by name; null/blank defaults to {@code "spinnaker"}.
   */
  public OpenFgaZanzibarStore(ZanzibarSchema zanzibarSchema, String apiUrl, String storeName) {
    this(zanzibarSchema, apiUrl, storeName, DEFAULT_LIST_DEADLINE);
  }

  /**
   * @param storeName store to reuse-or-create by name; null/blank defaults to {@code "spinnaker"}.
   * @param listDeadline the server's {@code listObjectsDeadline}.
   */
  public OpenFgaZanzibarStore(
      ZanzibarSchema zanzibarSchema, String apiUrl, String storeName, Duration listDeadline) {
    this.zanzibarSchema = zanzibarSchema;
    this.listDeadline = listDeadline;
    try {
      // The SDK's read timeout defaults to 10 s, which would end listings the server still allows.
      this.client =
          new OpenFgaClient(
              new ClientConfiguration()
                  .apiUrl(apiUrl)
                  .readTimeout(listDeadline.plus(LIST_DEADLINE_MARGIN)));
    } catch (Exception e) {
      throw new IllegalStateException("Failed to build OpenFGA client", e);
    }
    var name = blankToNull(storeName);
    this.storeName = name == null ? DEFAULT_STORE_NAME : name;
  }

  @Override
  public String providerId() {
    return "openfga";
  }

  @Override
  public int writeBatchSize() {
    return MAX_BATCH;
  }

  /**
   * Resolves the store (reused-or-created by name) and pins the authorization model. Fully
   * automatic: the current schema is compared against the store's latest model and reused when it
   * matches, so restarts and replicas don't pile up identical model versions; a new version is
   * written only when the schema actually changed.
   */
  @Override
  public void applySchema() {
    try {
      var storeId = findStoreByName(storeName);
      if (storeId == null) {
        storeId = client.createStore(new CreateStoreRequest().name(storeName)).get().getId();
        log.info("Created OpenFGA store {} named '{}'", storeId, storeName);
      } else {
        log.info("Reusing OpenFGA store {} named '{}'", storeId, storeName);
      }
      client.setStoreId(storeId);
      ensureModel();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to apply OpenFGA model", e);
    }
  }

  /**
   * Reuse the latest model when it already matches the current schema; write a new version only
   * when the store has no model yet or the schema changed. A read failure propagates rather than
   * being mistaken for "no model" — so a transient outage never spuriously writes a new version.
   */
  private void ensureModel() throws Exception {
    var desired =
        MAPPER.readValue(OpenFgaModel.json(zanzibarSchema), WriteAuthorizationModelRequest.class);
    // The list endpoint returns 200 + empty list on an empty store, but throws on a real failure —
    // so an exception here (can't read) propagates instead of falling through to a write.
    var models =
        client
            .readAuthorizationModels(new ClientReadAuthorizationModelsOptions().pageSize(1))
            .get()
            .getAuthorizationModels();
    var latest = models == null || models.isEmpty() ? null : models.get(0); // newest first
    if (latest == null) {
      writeModel(desired, "no existing model");
    } else if (matches(latest, desired)) {
      client.setAuthorizationModelId(latest.getId());
      log.info("Reusing OpenFGA authorization model {}", latest.getId());
    } else {
      writeModel(desired, "schema changed from model " + latest.getId());
    }
  }

  private void writeModel(WriteAuthorizationModelRequest desired, String reason) throws Exception {
    var modelId = client.writeAuthorizationModel(desired).get().getAuthorizationModelId();
    client.setAuthorizationModelId(modelId);
    log.info("Wrote OpenFGA authorization model {} ({})", modelId, reason);
  }

  private static boolean matches(
      AuthorizationModel latest, WriteAuthorizationModelRequest desired) {
    return Objects.equals(latest.getSchemaVersion(), desired.getSchemaVersion())
        && withoutUnset(latest.getTypeDefinitions())
            .equals(withoutUnset(desired.getTypeDefinitions()))
        && withoutUnset(nullToEmpty(latest.getConditions()))
            .equals(withoutUnset(nullToEmpty(desired.getConditions())));
  }

  /** {@code value} as JSON without null or empty-string fields; the server returns unset as "". */
  private static JsonNode withoutUnset(Object value) {
    JsonNode node = MAPPER.valueToTree(value);
    removeUnset(node);
    return node;
  }

  private static void removeUnset(JsonNode node) {
    if (node instanceof ObjectNode object) {
      var unset = new ArrayList<String>();
      object
          .fieldNames()
          .forEachRemaining(
              name -> {
                var child = object.get(name);
                if (child.isNull() || (child.isTextual() && child.textValue().isEmpty())) {
                  unset.add(name);
                }
              });
      object.remove(unset);
    }
    node.forEach(OpenFgaZanzibarStore::removeUnset);
  }

  /** Id of the first store with the given name, or null if none exists. */
  private String findStoreByName(String name) throws Exception {
    var response = client.listStores(new ClientListStoresOptions().name(name)).get();
    var stores = response.getStores();
    if (stores == null) {
      return null;
    }
    return stores.stream()
        .filter(store -> name.equals(store.getName()))
        .map(Store::getId)
        .findFirst()
        .orElse(null);
  }

  private static <K, V> Map<K, V> nullToEmpty(Map<K, V> map) {
    return map == null ? Map.of() : map;
  }

  private static String blankToNull(String value) {
    return value == null || value.isBlank() ? null : value;
  }

  @Override
  public void write(Collection<ZanzibarRelationship> tuples) {
    runBounded(writeCalls(tuples));
  }

  @Override
  public void delete(Collection<ZanzibarRelationship> tuples) {
    runBounded(deleteCalls(tuples));
  }

  @Override
  public boolean check(String userId, String type, String name, Authorization action) {
    return check(ref("user", userId), relation(action), ref(type, name));
  }

  @Override
  public boolean isMember(String userId, String groupId) {
    return check(ref("user", userId), "member", ref("group", groupId));
  }

  @Override
  public Set<String> groupsOf(String userId) {
    var groups = new LinkedHashSet<String>();
    try {
      String token = null;
      do {
        var options = new ClientReadOptions();
        if (token != null && !token.isEmpty()) {
          options.continuationToken(token);
        }
        // A filtered Read needs an object type; "group:" (type only) + user reads the user's direct
        // group memberships. Leaving the object empty is rejected by OpenFGA.
        var request =
            new ClientReadRequest().user(ref("user", userId)).relation("member")._object("group:");
        var response = client.read(request, options).get();
        for (var tuple : response.getTuples()) {
          var object = tuple.getKey().getObject();
          if (object.startsWith("group:")) {
            groups.add(idOf(object));
          }
        }
        token = response.getContinuationToken();
      } while (token != null && !token.isEmpty());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    } catch (ExecutionException | FgaInvalidParameterException e) {
      throw new IllegalStateException("OpenFGA read failed", e);
    }
    return groups;
  }

  @Override
  public Set<String> lookupResources(String userId, String type, Authorization action) {
    var request =
        new ClientListObjectsRequest()
            .user(ref("user", userId))
            .relation(relation(action))
            .type(type);
    var prefix = type + ":";
    var ids = new LinkedHashSet<String>();
    var streamError = new AtomicReference<Throwable>();
    var started = System.nanoTime();
    try {
      // Streamed: plain ListObjects truncates at the server's result cap.
      client
          .streamedListObjects(
              request,
              new ClientStreamedListObjectsOptions(),
              response -> {
                var object = response.getObject();
                ids.add(
                    ZanzibarIds.decode(
                        object.startsWith(prefix) ? object.substring(prefix.length()) : object));
              },
              streamError::set)
          .get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    } catch (ExecutionException | FgaInvalidParameterException e) {
      throw new IllegalStateException("OpenFGA streamedListObjects failed", e);
    }
    // In-stream errors reach only the error consumer; the future still completes normally.
    if (streamError.get() != null) {
      throw new IllegalStateException("OpenFGA streamedListObjects failed", streamError.get());
    }
    // The server ends a listing at its deadline without an error.
    if (System.nanoTime() - started >= listDeadline.toNanos()) {
      throw new IllegalStateException(
          "OpenFGA listing of " + type + " reached the list deadline; it may be truncated");
    }
    return ids;
  }

  @Override
  public Set<ZanzibarRelationship> read(String objectType, String objectId) {
    var result = new LinkedHashSet<ZanzibarRelationship>();
    var object = ref(objectType, objectId);
    try {
      String token = null;
      do {
        var options = new ClientReadOptions();
        if (token != null && !token.isEmpty()) {
          options.continuationToken(token);
        }
        var response = client.read(new ClientReadRequest()._object(object), options).get();
        for (var tuple : response.getTuples()) {
          var key = tuple.getKey();
          var parts = key.getObject().split(":", 2);
          result.add(
              ZanzibarRelationship.of(
                  parts[0],
                  ZanzibarIds.decode(parts[1]),
                  key.getRelation(),
                  subjectRef(key.getUser())));
        }
        token = response.getContinuationToken();
      } while (token != null && !token.isEmpty());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    } catch (ExecutionException | FgaInvalidParameterException e) {
      throw new IllegalStateException("OpenFGA read failed", e);
    }
    return result;
  }

  @Override
  public Set<String> objectIds(String objectType) {
    // OpenFGA has no "list objects of a type"; read all tuples and filter. Costly at scale — a
    // point in SpiceDB's favor, which lists a type's objects directly via ReadRelationships.
    var prefix = objectType + ":";
    var ids = new LinkedHashSet<String>();
    try {
      String token = null;
      do {
        var options = new ClientReadOptions();
        if (token != null && !token.isEmpty()) {
          options.continuationToken(token);
        }
        var response = client.read(new ClientReadRequest(), options).get();
        for (var tuple : response.getTuples()) {
          var object = tuple.getKey().getObject();
          if (object.startsWith(prefix)) {
            ids.add(ZanzibarIds.decode(object.substring(prefix.length())));
          }
        }
        token = response.getContinuationToken();
      } while (token != null && !token.isEmpty());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    } catch (ExecutionException | FgaInvalidParameterException e) {
      throw new IllegalStateException("OpenFGA read failed", e);
    }
    return ids;
  }

  @Override
  public boolean isEmpty() {
    try {
      return client
          .read(new ClientReadRequest(), new ClientReadOptions().pageSize(1))
          .get()
          .getTuples()
          .isEmpty();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    } catch (ExecutionException | FgaInvalidParameterException e) {
      throw new IllegalStateException("OpenFGA read failed", e);
    }
  }

  @Override
  public void apply(
      Collection<ZanzibarRelationship> writes, Collection<ZanzibarRelationship> deletes) {
    // Deletes first, then writes; each phase runs its chunks bounded-parallel. Not one atomic
    // transaction; the next reconcile of an object applies whatever is left.
    runBounded(deleteCalls(deletes));
    runBounded(writeCalls(writes));
  }

  private boolean check(String user, String relation, String object) {
    try {
      var request = new ClientCheckRequest().user(user).relation(relation)._object(object);
      return Boolean.TRUE.equals(client.check(request).get().getAllowed());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    } catch (ExecutionException | FgaInvalidParameterException e) {
      throw new IllegalStateException("OpenFGA check failed", e);
    }
  }

  private static List<List<ZanzibarRelationship>> chunk(Collection<ZanzibarRelationship> tuples) {
    var chunks = new ArrayList<List<ZanzibarRelationship>>();
    var current = new ArrayList<ZanzibarRelationship>(MAX_BATCH);
    for (var tuple : tuples) {
      current.add(tuple);
      if (current.size() == MAX_BATCH) {
        chunks.add(current);
        current = new ArrayList<>(MAX_BATCH);
      }
    }
    if (!current.isEmpty()) {
      chunks.add(current);
    }
    return chunks;
  }

  private static ClientTupleKey tupleKey(ZanzibarRelationship t) {
    return new ClientTupleKey()
        .user(subjectUser(t.getSubjectRef()))
        .relation(t.getRelation())
        ._object(ref(t.getObjectType(), t.getObjectId()));
  }

  private static ClientTupleKeyWithoutCondition tupleKeyWithoutCondition(ZanzibarRelationship t) {
    return new ClientTupleKeyWithoutCondition()
        .user(subjectUser(t.getSubjectRef()))
        .relation(t.getRelation())
        ._object(ref(t.getObjectType(), t.getObjectId()));
  }

  private List<WriteCall> writeCalls(Collection<ZanzibarRelationship> tuples) {
    var calls = new ArrayList<WriteCall>();
    for (var batch : chunk(tuples)) {
      var keys = batch.stream().map(OpenFgaZanzibarStore::tupleKey).toList();
      calls.add(() -> client.write(new ClientWriteRequest().writes(keys), writeOptions));
    }
    return calls;
  }

  private List<WriteCall> deleteCalls(Collection<ZanzibarRelationship> tuples) {
    var calls = new ArrayList<WriteCall>();
    for (var batch : chunk(tuples)) {
      var keys = batch.stream().map(OpenFgaZanzibarStore::tupleKeyWithoutCondition).toList();
      calls.add(() -> client.write(new ClientWriteRequest().deletes(keys), writeOptions));
    }
    return calls;
  }

  /** Issue the write requests with up to {@link #MAX_PARALLEL} in flight, then await them all. */
  private static void runBounded(List<WriteCall> calls) {
    var permits = new Semaphore(MAX_PARALLEL);
    var inFlight = new ArrayList<CompletableFuture<?>>(calls.size());
    for (var call : calls) {
      permits.acquireUninterruptibly();
      CompletableFuture<?> future;
      try {
        future = call.execute();
      } catch (FgaInvalidParameterException e) {
        permits.release();
        throw new IllegalStateException("OpenFGA write failed", e);
      }
      inFlight.add(future.whenComplete((result, error) -> permits.release()));
    }
    try {
      CompletableFuture.allOf(inFlight.toArray(CompletableFuture[]::new)).get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    } catch (ExecutionException e) {
      throw new IllegalStateException("OpenFGA write failed", e.getCause());
    }
  }

  @FunctionalInterface
  private interface WriteCall {
    CompletableFuture<?> execute() throws FgaInvalidParameterException;
  }

  /**
   * OpenFGA group subjects use the {@code group:x#member} userset; users and wildcards pass
   * through.
   */
  private static String subjectUser(String ref) {
    var colon = ref.indexOf(':');
    var type = ref.substring(0, colon);
    var user = ref(type, ref.substring(colon + 1));
    return "group".equals(type) ? user + "#member" : user;
  }

  /** Inverse of {@link #subjectUser}: canonicalize a stored user back to a subject ref. */
  private static String subjectRef(String user) {
    var bare =
        user.endsWith("#member") ? user.substring(0, user.length() - "#member".length()) : user;
    return bare.substring(0, bare.indexOf(':')) + ":" + idOf(bare);
  }

  private static String ref(String type, String id) {
    return type + ":" + ZanzibarIds.encode(id, ID_CHARS);
  }

  /** The decoded id of a {@code type:id} reference. */
  private static String idOf(String ref) {
    return ZanzibarIds.decode(ref.substring(ref.indexOf(':') + 1));
  }

  private static String relation(Authorization action) {
    return action.name().toLowerCase(Locale.ROOT);
  }

  @Override
  public void close() {
    // OpenFgaClient holds no closeable resource beyond its OkHttp pool, which idles out.
  }
}
