# kork-zanzibar

Store-backed authorization for Spinnaker: the decision layer over a relationship store, exposed as
one `ZanzibarStore` interface. Two engine adapters — SpiceDB and OpenFGA — sit behind it so the engine
choice stays swappable. Fiat consumes it (via `fiat-zanzibar`) as its `PermissionsRepository`, so
the standard Spring Security `PermissionEvaluator` seam every service already uses is unchanged.

The API does not depend on Fiat's model: resource types are strings (for example `application`),
actions are kork's `Authorization`, and a resource's grants are a map from action to roles, where no
roles at all means unrestricted.

## Why a store

Fiat materializes a full per-user permission blob and caches it. That does not scale to 10k+ roles,
goes stale, and couples every service to Fiat's availability. A relationship store instead:

- **Checks by identity.** The caller sends only who they are; the store expands their roles and
  nested groups server-side. The identity token never carries the role set.
- **Writes once, when data is authored.** A membership or ACL change is one small write, not a
  rebuild of every affected user's permissions.
- **Owns ACLs off the request path.** No Spinnaker service sits on the zanzibar hot path — only the
  store. Resource permissions are written into it by change events from their authoring services.

## Layers

| Class | Role |
|---|---|
| `ZanzibarStore` | Engine-agnostic store: batch `write`/`delete` of `ZanzibarRelationship` tuples, `check`, `isMember`, `lookupResources`. |
| `CachingZanzibarStore` | Read-through TTL cache over the hot decision path (`check` / `isMember` / `lookupResources`); wrap the engine adapter on the read path. |
| `RateLimitedZanzibarStore` | Limits write requests per second for bulk writes (bootstrap, benchmarks), counting `writeBatchSize()` relationships per request. Reads are not limited. |
| `consistency/ConsistencyTokens` | A store's newest consistency token: its reads use it, and its writes record theirs. |
| `consistency/ConsistencyStrategy` | Shares that token between replicas: `heartbeat`, `watch` or `shared-sql` (`SharedConsistencyTokens`). |
| `ZanzibarRelationship` | The Zanzibar tuple primitive both adapters translate. |
| `ZanzibarIds` | Reversible `=XX` escaping of ids into each engine's id charset (emails, URN-style ids, dotted names). |
| `ZanzibarSchema` | The model: the resource types to define and their actions. `SpiceDbSchema` and `OpenFgaModel` render it for each engine. |
| `SpiceDbZanzibarStore` / `OpenFgaZanzibarStore` | Engine adapters (authzed v1 gRPC / openfga-sdk HTTP). |
| `ingest/RoleSource`, `ingest/ResourceSource` | Streaming inputs (adapt a role provider / resource authority, or a generator). |
| `ingest/IngestionPipeline` | Reads sources and writes relationships in flushed batches — the analogue of Fiat's role/resource sync. |
| `ingest/Reconciler` | Updates/deletes: diffs an object's current tuples against the desired set and applies the delta atomically. |
| `ingest/ResourceChangeNotifier` | Change hook: a per-object-locked reconcile of one object. With a same-thread executor (as Fiat runs it), failures reach the caller. |
| `ingest/ReconcileLock` + `InProcessReconcileLock` | Serializes work by key — single-flight bootstrap and per-object reconcile. In-process default; bind to a distributed lock in production. |
| `ingest/LockManagerReconcileLock` | Cross-instance `ReconcileLock` backed by kork's distributed `LockManager` (Redis). |

### Two role-population paths (both Fiat paths preserved)

- **Directory-synced roles** — `RoleSource` → `IngestionPipeline.ingestRoles` writes group
  memberships (nested `group#member` handles the 10k-role scale without enumeration).
- **Login-asserted roles** — the bounded IdP-claim set a user presents at login. Fiat resolves them
  and stores the result; `fiat-zanzibar`'s `ZanzibarPermissionsRepository.put` writes them as the
  user's memberships via the per-user reconcile, so a check never needs per-request asserted roles:
  the store already holds them as durable memberships and expands them by identity like any other
  role.

### Resource ingestion

`ResourceSource` emits each resource with its grants. `IngestionPipeline.ingestResources` expands
them: grants that name roles become one ACL relationship per `(action, role)`; grants that name no
roles become a `user:*` grant on every action (Fiat's "unrestricted"). The schema defines whatever
types its caller lists; Fiat lists one per `ZanzibarResourceType` bean: `account`, `application`,
and `build_service`.

## Keeping the store in sync

Two paths keep the store current, both built on `Reconciler`:

- **Change hook** (`ResourceChangeNotifier`) — called for each changed resource or group
  (`resourceChanged` / `resourceDeleted` / `groupChanged`) with its new state. The reconcile is
  per-object-locked, so concurrent changes to one object serialize. In Fiat, sources publish changes
  to `POST /zanzibar/resources/{type}`; Fiat derives the effective permissions with its own
  providers and runs the hook on the request thread, so a failure returns an error and the sender
  retries.
- **Bootstrap** (`fiat-zanzibar`'s `ZanzibarBootstrap`) — lists every stored type from Fiat's
  providers, reconciles each resource, and deletes orphans (objects the store still holds but no
  provider lists). A type with no provider is skipped. It runs only when an admin calls Fiat's
  internal `POST /zanzibar/bootstrap?rate=<write requests per second>` (default 1,000), never at
  startup; there is no periodic sweep. Orphan detection needs listing a type's objects: clean on
  SpiceDB (`ReadRelationships` by type), but OpenFGA has no list-objects-of-type, so its `objectIds`
  reads the whole store and filters.

No message bus: sources call Fiat directly. A bus would add a hop and a dependency with a single
consumer.

## Caching

The store answers checks live, so the hot read path is cached to cut store load. Wrap the engine
adapter in `CachingZanzibarStore` on the read path:

```java
ZanzibarStore store = new CachingZanzibarStore(new SpiceDbZanzibarStore(...), Duration.ofSeconds(10), 50_000);
```

It caches, with a short TTL:

- **decisions** — `check`, the per-`@PreAuthorize` hot path;
- **admin resolution** — `isMember`, called once per user permission lookup;
- **enumeration** — `lookupResources` for UI list/filter.

The trade is bounded staleness: a change takes effect within the TTL rather than instantly. A
write through the same instance evicts the caches; cross-instance writes are bounded by the TTL
(SpiceDB's Watch API could drive precise invalidation later).

Deliberately **not** cached: `read` / `objectIds` (reconciliation needs current state). The
providers' listings during a bootstrap bypass this cache too (the bootstrap wants truth). The
engines also cache sub-problems internally beneath this layer.



Every Spinnaker service runs many replicas, so the sync path is built for it:

- **The bootstrap is single-flight.** `ZanzibarBootstrap.run(rate)` sweeps under one global
  `ReconcileLock` key, so exactly one leader runs it at a time instead of N replicas duplicating the
  scan. Bind `ReconcileLock` to a distributed lock (kork's Redis `LockManager`) in production;
  `InProcessReconcileLock` covers a single instance and tests.
- **Events are at-least-once and idempotent.** Because a handler reconciles to *current truth*
  (the event is just a trigger + id), duplicate and out-of-order events converge to the same state,
  and duplicate emitters across source replicas are harmless.
- **Same-object reconciles are serialized.** The handler holds a per-object `ReconcileLock` key, so
  two concurrent events for one object don't race (and the idempotent diff never issues a duplicate
  write, which OpenFGA rejects). With the distributed lock this holds across instances.
- **Residual drift is corrected by the next change.** If truth changes mid-reconcile between two
  instances and a stale write lands, the object's next event or the next bootstrap reconciles it.

### Consistency tokens

A store's reads use its newest consistency token (`ConsistencyTokens`), so they see every write up
to it; its writes and deletes record their response's token. One strategy per engine shares the
token between replicas, set with `fiat.zanzibar.<engine>.consistency.strategy`:

| strategy | how | other replicas see a write |
|---|---|---|
| `none` | no tokens; the engine's default reads | – |
| `local` | own writes only | – |
| `heartbeat` | adds or removes a marker grant on `application:__zanzibar_consistency` every `heartbeat-interval` (5 s); each write returns a fresh token | within `heartbeat-interval` |
| `watch` | follows the engine's change stream; reconnects from the newest token with backoff | at once |
| `shared-sql` | offers and reads the newest token in Fiat's `fiat_zanzibar_consistency_token` table every `poll-interval` (1 s) | within about two poll intervals |

- A token received more than `max-age` (1 minute) ago is dropped, and reads go without one.
- SpiceDB's default is `none`. With a token, its checks and lookups read `at_least_as_fresh`;
  without one, fully consistent. Reconciliation reads stay fully consistent. Its tokens are
  opaque, so they're ordered by arrival. Its `watch` needs `track_commit_timestamp=on` in Postgres.
- OpenFGA has no consistency tokens.
- `shared-sql` needs Fiat's SQL database (`sql.enabled=true`); without it, Fiat fails at startup.



Requires a reachable Docker daemon (pulls `authzed/spicedb` and `openfga/openfga` via Testcontainers):

```
./gradlew :kork:kork-zanzibar:test
```

`ZanzibarStoreComparisonTest` runs four scenarios against both engines:

1. **Core model** — identity-only checks, nested groups, `user:*` wildcard, per-action
   independence, revocation on the next check.
2. **Every resource type** — ingests fixtures through the pipelines and verifies the full access
   matrix for a variety of users (nested-group member, direct member, other-team, no-roles) across
   all eight resource types, plus admin-group membership resolution.
3. **Scale ingest** — generates a large nested group graph and resource set, times the ingest, and
   verifies a deterministic probe. Tune with system properties:

   ```
   ./gradlew :kork:kork-zanzibar:test \
     -Dzanzibar.scale.groups=10000 -Dzanzibar.scale.users=2000 -Dzanzibar.scale.resourcesPerType=2000
   ```

4. **Enumeration** — `lookupResources` returns exactly the resources of a type a user may act on.

`ReconciliationTest` covers the update/delete diffing, and `fiat-zanzibar`'s `ZanzibarBootstrapTest`
covers the bootstrap's reconcile with orphan deletion under the single-flight lock — each against
both engines.
`fiat-zanzibar`'s `ZanzibarAuthorizationEndToEndTest` drives the whole chosen path (reconcile resources +
per-user memberships → `ZanzibarPermissionsRepository` reads, admin, and the rebuilt View) against
both engines.

### Benchmarks

`ZanzibarStoreBenchmark` load-tests each engine at Fiat's target scale — thousands of users, up to 10k
roles each, thousands of resources per type — and reports ingest throughput plus latency
percentiles (p50/p95/p99) for the hot paths a running Fiat drives: point `check`, `lookupResources`
(View enumeration, once per type per login), and `isMember` (admin resolution). It runs against the
raw adapters (no cache), so the numbers reflect the store itself. It is expensive and gated behind
`-Dzanzibar.benchmark=true` (so it never runs in the normal suite); defaults are modest, and the
`zanzibar.bench.*` knobs dial it up (forwarded to the test JVM by `kork-zanzibar.gradle`):

```
./gradlew :kork:kork-zanzibar:test --tests "*ZanzibarStoreBenchmark" \
  -Dzanzibar.benchmark=true \
  -Dzanzibar.bench.groups=20000 -Dzanzibar.bench.users=2000 \
  -Dzanzibar.bench.rolesPerUser=10000 -Dzanzibar.bench.resourcesPerType=3000
```

Writes are limited to `-Dzanzibar.bench.rate` requests per second (1,000).

The docker-free tests — `CachingZanzibarStoreTest`, `ResourceChangeNotifierTest`,
`UserMembershipReconcileTest`, and `fiat-zanzibar`'s `ZanzibarPermissionsRepositoryTest` — run standalone:

```
./gradlew :kork:kork-zanzibar:test --tests "*CachingZanzibarStoreTest"
./gradlew :fiat:fiat-zanzibar:test --tests "*ZanzibarPermissionsRepositoryTest"
```

## Engine comparison notes

- **Consistency** — SpiceDB reads are `fullyConsistent` unless a consistency strategy supplies a
  ZedToken (see [Consistency tokens](#consistency-tokens)); OpenFGA reads latest by default.
- **Batch limits** — adapters chunk writes to each engine's per-transaction cap (SpiceDB 500,
  OpenFGA 100).
- **Transport / datastore** — SpiceDB gRPC, OpenFGA HTTP; both run on Postgres/MySQL.
- **Id charsets** — SpiceDB accepts only `[a-zA-Z0-9/_|\-=+]`; OpenFGA rejects `:` and `#`. Each
  adapter escapes rejected bytes with `ZanzibarIds`.
- **Listing** — OpenFGA's `ListObjects` stops at its result cap (1,000 by default) without an error,
  so the adapter uses `StreamedListObjects`. The server also ends a streamed listing at its list
  deadline (3 s by default) without an error; the adapter fails any listing that runs that long.
  Set the server's deadline well above the slowest listing, and pass the same value to the adapter.

### Updating and deleting

Because the store holds durable tuples (unlike Fiat, which recomputes a per-user blob each sync),
changed permissions must be **reconciled**, not just re-written — otherwise a dropped grant lingers.
`Reconciler` handles this per object: read the object's current tuples, diff against the desired
set derived from its current grants, and apply adds + removes in one atomic transaction
(`ZanzibarStore.apply`). `reconcileResource` covers permission updates and restricted↔unrestricted
transitions, `deleteResource` removes every grant, and `reconcileGroup` handles membership changes.
`ReconciliationTest` proves each case (including the over-grant bug: shrinking a resource's roles
revokes the removed ones) against both engines.

## Wiring into Spinnaker (via Fiat)

Rather than embed the engine client in every service, Fiat is reused as the zanzibar service it already
is: the `fiat/fiat-zanzibar` module swaps Fiat's backend by config. Services keep calling Fiat through
`fiat-api`; the services that author permissions also publish change events to Fiat, behind
`services.fiat.resource-events.enabled`.

- `fiat-zanzibar` depends on `kork-zanzibar`; `FiatZanzibarConfig` (auto-configured, gated by
  `fiat.zanzibar.enabled=true`) builds the store from the engine `fiat.zanzibar.engine` selects
  (`SpiceDbEngineConfig` or `OpenFgaEngineConfig`, each with its own `fiat.zanzibar.<engine>.*`
  settings), adds `CachingZanzibarStore`, and makes `ZanzibarPermissionsRepository` the primary
  `PermissionsRepository`.
- **Schema/store are provisioned automatically at startup** (`applySchema`), no manual step:
  SpiceDB's schema write is idempotent; OpenFGA reuses-or-creates its store by name (default
  `spinnaker`) and compares the current schema to the store's latest authorization model — reusing
  that model when it matches (so restarts and replicas don't pile up identical versions) and writing
  a new version only when the store has no model yet or the schema actually changed. A model/store
  that exists but **can't be read** fails startup rather than writing a spurious version — only a
  genuine absence creates one.
- `ZanzibarPermissionsRepository.get` builds a user's `UserPermission` on demand from concurrent
  `lookupResources` calls. Each resource's authorizations ride on one of the user's roles, so the View
  is correct without enumerating 10k roles. Admin and account-manager status come from the user's
  roles and Fiat's `fiat.admin.roles` / account-manager roles, as in Fiat's resolver.
- **One flag.** `fiat.zanzibar.enabled=true` is the only switch. `FiatZanzibarEnvironmentPostProcessor`
  turns the periodic role syncs off ahead of all other configuration, logging any setting it
  overrides, and defaults the Redis permissions repository off.
- **Store population** is its own runtime, `FiatZanzibarSyncConfig`, gated on the same
  `fiat.zanzibar.enabled` and independent of the legacy write-mode:
  - `ZanzibarResourceEventsController` receives change events and derives permissions with Fiat's
    `ResourcePermissionProvider` beans, through one `ZanzibarResourceType` bean per stored type
    (`FiatZanzibarTypesConfig`).
  - `ZanzibarBootstrap` lists each stored type from Fiat's **authenticated** `ResourceProvider`
    beans at startup, reconciles it, deletes orphans, then reconciles every service account's
    memberships.
  - `ServiceAccountMembershipReconciler` keeps service accounts' memberships equal to their
    Front50 `memberOf`.
- **Login is Fiat's own.** `RolesController` resolves the user's roles and stores the result;
  `ZanzibarPermissionsRepository.put` writes them as memberships. The store lists no users, so
  `/roles/sync` refreshes only service accounts; `/roles/sync/serviceAccount/{id}` writes that
  account's memberships. Logout keeps memberships until the next login replaces them.
- **Real role names.** The View's roles are the user's store memberships, so `hasAnyRole` works.
- Only Fiat pulls the engine client — services stay light.

## Remaining caveats

- **Service accounts are not stored as ACLs.** Which service accounts a user may use comes from
  Fiat's service-account provider and its access predicates, as in stock Fiat; `service_account` is
  excluded from the bootstrap's resource pass since its access derives from `memberOf`.
- **Runtime validation** — the Docker-backed tests (`ZanzibarStoreComparisonTest`,
  `ZanzibarAuthorizationEndToEndTest`) pass against both engines, but behavior against a running
  Fiat deployment is still unverified.

## Authentication → authorization

Identity is established at Gate (X.509 / SAML / OAuth / ...) and propagates to each service via the
`X-SPINNAKER-USER` header — no internal identity tokens. Each service authorizes exactly as it
always has: `@PreAuthorize` checks go through fiat-api's `FiatPermissionEvaluator`, which asks Fiat.
Fiat now answers from the store (`ZanzibarPermissionsRepository`) by that identity, and the store
expands the user's roles and nested groups server-side. Roles never travel in the request — only
identity does.

**Trust model:** unchanged. Service-to-service authentication is out of scope, and the
`X-SPINNAKER-USER` header is trusted as it is today. Gate is the edge: it authenticates the human
and sets the header, and must reject any client-supplied identity header.
