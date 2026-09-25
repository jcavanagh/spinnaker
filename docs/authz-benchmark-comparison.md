# Fiat authorization backend benchmarks

Load comparison of authorization backends for Spinnaker Fiat at production scale:

- **Fiat's repositories**: the precomputed `PermissionsRepository`, on **Redis** and **Postgres**.
- **Zanzibar** (`kork-zanzibar` + `fiat-zanzibar`): a relationship store, with **SpiceDB** and
  **OpenFGA** adapters.
- **Casbin**: an embedded jCasbin prototype. Rejected; its numbers are kept for reference, and its
  code is not included.

Measured with each module's gated JVM benchmark, against Postgres, Redis, SpiceDB, and OpenFGA
containers on one Docker host. Numbers compare backends; they are not tuned-cluster capacity, and
most rows come from one run.

The design is described in `docs/rfc-fiat-store-backed-authorization.md`.

## Terms

- **View**: a user's roles and every resource and action they can reach. Each service fetches it
  from Fiat on a `fiat-api` cache miss and checks it in process.
- **Point check**: whether one user may take one action on one resource.
- **Lookup**: Zanzibar's `lookupResources`, which lists the resources of one type a user may take
  one action on. A View build makes one per type and action, plus a membership read: its store
  calls.
- **Unrestricted resource**: a resource with no roles, which every user can reach.
- **Full role sync**: Fiat's periodic rebuild of every user's permissions from their stored roles.
- **Re-login**: a login by a user whose memberships are already stored.
- **Bootstrap**: the one-time load of every relationship into a Zanzibar store.
- **n**: the samples behind a row's percentiles, within one run.

## Production sizing

Roles per user at login in a large deployment:

| population | source | median | avg | p95 | p99 | max |
|---|---|---|---|---|---|---|
| **human (SSO)**, ~2,500 users | SSO group membership at login | **~3,500** | **~5,000** | ~12,000 | ~14,000 | **~21,000** |
| service/bot (X509 certs), ~100 principals | X509 client-cert login | ~100 | ~250 | ~900 | ~1,000 | ~1,100 |

Inventory: ~2,500 users, ~10,000 accounts, ~7,000 applications.

## Benchmark parameters

Every backend runs at the same scale: **2,500 users × 5,000 roles** (drawn from **20,000 groups**),
**10,000 accounts + 7,000 applications**, each resource granting READ to 5 groups and WRITE to 1,
with 5% unrestricted. The Fiat baseline also has 500 service accounts.

The Fiat baseline runs Fiat's real code paths:

| operation | code path |
|---|---|
| login | `DefaultPermissionsResolver.resolveAndMerge` with login-asserted roles, then `PermissionsRepository.put` |
| full role sync | `Synchronizer.syncAndReturn`, run twice (it replays each user's stored roles) |
| service-account sync | `Synchronizer.syncServiceAccount` |
| read | `PermissionsRepository.get` + `getView()` |

The Postgres runs use these SQL settings: async pool 10, read batch 1,000, write batch 4,000.

Unless noted, results include a one-method fix to `Permissions.getAuthorizations(Set<Role>)`, which
otherwise copies the user's role names for every resource it checks. See "Fiat as released" below.

## Where each backend does its work

Fiat's repositories and Zanzibar spend their effort at different times, so a fair comparison counts
both sides:

- **Fiat's repositories precompute.** Each user's full permissions are rebuilt at login and again
  in each full role sync. A read is then one fetch of data computed earlier. A resource ACL change
  reaches users at the next sync.
- **Zanzibar computes at read time.** The store holds memberships and ACLs. Each is written once,
  when it is authored. A user's permissions are built from the store when Fiat is asked for them
  (on a `fiat-api` cache miss).

## Results: Fiat's repositories

| repository | login resolve p50 | login persist p50 | full role sync (3,000 principals) | service-account sync p50 | get + View p50 / p95 |
|---|---|---|---|---|---|
| **Redis** | 14.9 ms | 22.8 ms | **21–27 s** | 1.8 s | **37 ms / 43 ms** |
| **Postgres** | 23.7 ms | **268 ms** | **27–28 s** | **5.4 s** | **44 ms / 63 ms** |

n per row: 2,500 logins, 2 full syncs (each range spans both), 10 service-account syncs and 500
reads. A service-account sync re-resolves every user who shares a role with the account (158 on
average here).

**Fiat as released**, without the fix, measured with 100 users at the same role and resource scale
(per-user costs do not depend on the user count):

| repository | login resolve p50 | login persist p50 | full role sync (100 users + 500 service accounts) | service-account sync p50 | get + View p50 |
|---|---|---|---|---|---|
| **Redis** | **2.67 s** | 25 ms | 55 s | 6.3 s | **2.18 s** |
| **Postgres** | **2.41 s** | 276 ms | 52 s | 6.3 s | **2.46 s** |

n per row: 100 logins, 10 service-account syncs and 500 reads. A full sync re-resolves at about
0.5 s per user, roughly 22 minutes for 2,500 users.

## Results: Zanzibar View build

The View build makes 13 store calls: 12 `lookupResources` (3 resource types × 4 actions) and the
user's memberships. Fiat's controller then computes the View from the result.

**SpiceDB, full scale** (every accessible resource returned: 14,125 of 14,125):

| operation | n | p50 | p95 |
|---|---|---|---|
| **View build: store reads + View computation** (what `/authorize` returns) | 10 | **452 ms** | 535 ms |
| View build: store reads only (concurrent lookups, 32 threads) | 10 | 448 ms | 529 ms |
| same, with Fiat as released (no role-lookup fix) | 10 | 3.12 s | 3.78 s |
| re-login: membership write, no change | 5 | 75 ms | 84 ms |
| re-login: first store reads after the write | 5 | 413 ms | 479 ms |
| store reads, warm local cache (10 s TTL) | 5 | 81 ms | 84 ms |

With the fix, computing the View adds about 4 ms. The store reads take about as long as the slowest
single lookup, so more threads do not help.

**OpenFGA** cannot serve the View reliably (measured with the same adapter code):

| OpenFGA configuration, full scale | n | View p50 | View p95 | resources returned |
|---|---|---|---|---|
| `ListObjects`, server defaults | 10 | 2.1 s | 29.5 s | **22%** (3,096 of 14,149) |
| `StreamedListObjects`, 120 s list deadline, read timeout raised to match (2026-10-03) | 10 | **499 ms** | 28.1 s | 100% (14,149 of 14,149) |
| SpiceDB, same run (2026-10-03) | 10 | 415 ms | 510 ms | 100% |

Both OpenFGA limits cut results short **without an error**: `ListObjects` stops at its result cap
(1,000), and the streamed API stops at its list deadline (3 s by default). The adapter uses the
streamed API and fails any listing that reaches the deadline.

The SDK's 10 s read timeout first ended listings that the 120 s deadline allowed; the adapter now
sets the read timeout from the deadline. Then every View was complete, with a median close to
SpiceDB's, but one of 10 took 28 s. Which one is not recorded; a first, cold View is likely. Its
other operations were slower than SpiceDB's in the same run: re-login writes took 263 ms (SpiceDB
73 ms), the first View after a write 465 ms (406 ms), and a warm-cache View 255 ms (75 ms). The
ingest took 403 s.

Earlier runs at medium scale (20 users × 2,000 roles from 4,000 groups, with 4,000 accounts and
3,000 applications) returned 78% of resources at the default 3 s deadline. With a 120 s deadline
they were complete, but took 7.4–8.5 s against SpiceDB's 231 ms.

## Results: point checks

| backend | point check p50 | point check p95 | admin check p50 |
|---|---|---|---|
| **Zanzibar SpiceDB** | 0.84 ms | 1.66 ms | 0.59 ms |
| **Zanzibar OpenFGA** | 0.79 ms | 1.12 ms | 0.45 ms |
| **Casbin** (prototype) | 4,435 ms | 4,786 ms | 19 ms |

n per engine: 2,000 point checks and 2,000 admin checks. Fiat makes no per-check store call: each
service checks its cached View in process. `fiat-api`
does not make point-check calls today, so these numbers matter only for a future point-check mode.

## Results: bootstrap load

After the bootstrap, the store changes one object at a time. Each range spans the full-scale runs.

| engine | 12.5M memberships + 100k ACLs |
|---|---|
| **SpiceDB** | 567–592 s |
| **OpenFGA** | 354–403 s |

## Real identifier formats

User ids are often emails, groups are often numeric ids, X509 logins use the email as a role, and
account names contain dots. URN-style person and group ids (`urn:…:person:123`) are also valid
principals.

| identifier | SpiceDB, unescaped | OpenFGA, unescaped | both, with id escaping |
|---|---|---|---|
| email user id | rejected | accepted | accepted |
| URN-style user id | rejected | rejected | accepted |
| email used as a role | rejected | accepted | accepted |
| dotted account name | rejected | accepted | accepted |
| numeric group, hyphenated app | accepted | accepted | accepted |

The adapters escape each byte an engine rejects as `=XX` (see the RFC).

## Interpretation

- **Fiat's fast read is paid for at write time.** The 37–44 ms read returns data rebuilt by a full
  sync that takes 27–28 s on Postgres (21–27 s on Redis), plus 268 ms of writes per Postgres login
  and 5.4 s per service-account change. Resource ACL changes wait for the next sync.
- **Fiat as released spends seconds per user at this role count**, in both resolution and View
  computation. The role-lookup fix removes that for every backend.
- **Zanzibar pays at read time instead.** On SpiceDB an uncached View takes 452 ms, and `fiat-api`
  caches it. Writes are small and happen when data changes: a login writes its memberships in
  75 ms, and a resource event rewrites one object.
- **SpiceDB is the reliable Zanzibar engine for the View.** OpenFGA silently truncates by default.
  With a raised list deadline its median View was close to SpiceDB's, but one View in 10 took 28 s,
  for a reason not yet known.
- **Casbin is not viable.** A point check takes 4.4 s at this role count.
- **Next steps for the View:** time each lookup to find the heavy one, tune SpiceDB, or add a
  point-check mode to `fiat-api`, which removes the View from the per-request path entirely.

## Reproduce

Each benchmark is gated and needs Docker (Testcontainers). `…bench.heap` sets the forked test JVM's
heap.

```
# Fiat baseline (redis | mysql | postgres); SQL settings default to the values above
./gradlew :fiat:fiat-sql:test --tests "*FiatBaselineBenchmark" -Dfiat.benchmark=true \
  -Dfiat.bench.backend=postgres -Dfiat.bench.users=2500 -Dfiat.bench.rolesPerUser=5000 \
  -Dfiat.bench.groups=20000 -Dfiat.bench.accounts=10000 -Dfiat.bench.applications=7000 \
  -Dfiat.bench.heap=8g

# Zanzibar View build (spicedb,openfga); -Dfiat.zanzibar.bench.lookupParallelism sets the lookup
# pool. OpenFGA beyond small scale needs -Dfiat.zanzibar.bench.openfgaListDeadline=120s, or its
# listings fail at the default 3 s deadline
./gradlew :fiat:fiat-zanzibar:test --tests "*ZanzibarViewBenchmark" -Dfiat.zanzibar.benchmark=true \
  -Dfiat.zanzibar.bench.engines=spicedb -Dfiat.zanzibar.bench.users=2500 \
  -Dfiat.zanzibar.bench.rolesPerUser=5000 -Dfiat.zanzibar.bench.groups=20000 \
  -Dfiat.zanzibar.bench.accounts=10000 -Dfiat.zanzibar.bench.applications=7000 \
  -Dfiat.zanzibar.bench.heap=8g

# Zanzibar point checks (both engines)
./gradlew :kork:kork-zanzibar:test --tests "*ZanzibarStoreBenchmark" -Dzanzibar.benchmark=true \
  -Dzanzibar.bench.datastore=postgres -Dzanzibar.bench.users=2500 -Dzanzibar.bench.rolesPerUser=5000 \
  -Dzanzibar.bench.groups=20000 -Dzanzibar.bench.resourcesPerType=4250
```

Java 25 is required to build; run Gradle with `JAVA_HOME` pointing at a JDK 25.
