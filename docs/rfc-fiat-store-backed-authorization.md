# Store-backed authorization for Fiat

| | |
|-|-|
| **Status**     | Proposed |
| **RFC #**      | TBD (assign at PR) |
| **Author(s)**  | Joe Cavanagh (`@jcavanagh`) |

## Overview

Fiat stores one precomputed `UserPermission` per user in Redis or SQL: the user's roles and every
resource they can access. It rebuilds each one at login and in a periodic full role sync. Users of a
large deployment belong to **thousands** of SSO groups (median ~3,500, tail >20,000), so this model
does a large amount of repeated work. At that scale a full sync on Postgres takes 27–28 s, and
resource changes wait for the next sync.

This RFC adds a **relationship store** (the Google Zanzibar model; SpiceDB recommended) as a Fiat
`PermissionsRepository`. Fiat's API is unchanged, so `fiat-api` and every service that uses it are
unchanged. Each piece of authorization data is written to the store once, where it is authored:

- memberships at login, through Fiat's own login path;
- applications and service accounts when Front50 saves them;
- accounts when Clouddriver's account management saves them.

As future work, group memberships need not come from Spinnaker at all. Another service connected to
the store, such as a directory sync, could manage them. Spinnaker would then store no roles, only
principals, and the store would relate each principal to its groups from that externally loaded
data.

Fiat builds each user's `UserPermission` from the store when it is asked for it.

### Goals and Non-Goals

**Goals**
- Remove the full role sync and the per-user precomputed permissions it maintains.
- Apply resource ACL changes when they are authored, not at the next sync.
- Keep Fiat's HTTP API and the `fiat-api` client contract unchanged.
- Keep the relationship engine swappable (SpiceDB / OpenFGA) behind one interface.
- Ship dark behind flags, with the existing repositories as the rollback.

**Non-Goals**
- Changing authentication (SAML / X509 / OAuth at Gate) or the internal identity header.
- Changing how memberships are refreshed. As today, a user's memberships change when they log in.
- Service-to-service authentication.
- Fine-grained authorization beyond Fiat's current RBAC.

## Motivation and Rationale

**Current state.** Fiat's `PermissionsRepository` holds one precomputed `UserPermission` per user.
It is rebuilt at login and by a full role sync, and `fiat-api` fetches it whole into each service.
The data is duplicated across users (each holds every resource it can access), and a change to a
resource's ACL reaches users only at the next sync.

**Measured cost at scale** (2,500 users × 5,000 roles, 17,000 resources; full data in
`docs/authz-benchmark-comparison.md`):

| Fiat operation | Redis | Postgres |
|---|---|---|
| login: resolve + persist, p50 | 14.9 ms + 22.8 ms | 23.7 ms + 268 ms |
| full role sync, 3,000 principals | 21–27 s | **27–28 s** |
| service-account sync, p50 | 1.8 s | 5.4 s |
| read (`get` + View), p50 | 37 ms | 44 ms |

Fiat's reads are fast because the work was already done at write time. That work grows with users ×
accessible resources.

These numbers include the role-lookup fix in `Permissions.getAuthorizations`, now merged.

**Benefit for operators/users:** no full sync, resource changes take effect when they are made, and
a store that grows with memberships + ACLs rather than users × resources. As future work, with
memberships managed outside Spinnaker, Spinnaker would store no roles at all, only principals.

## Design

### Model

Authorization data is stored as relationship tuples:
- **memberships:** `group:<role>#member@user:<id>`
- **resource ACLs:** `<type>:<name>#<action>@group:<role>#member`, or `@user:*` for unrestricted
  resources

`kork-zanzibar` does not depend on Fiat: resource types are strings, actions are kork's
`Authorization`, and a resource's grants are a map from action to roles. `fiat-zanzibar` converts
Fiat's model at the boundary. `ZanzibarSchema` lists the types Fiat stores (`account`,
`application`, `build_service`); each engine adapter renders it into its own schema and applies
that at startup.

**Identifiers.** Engines limit id characters. SpiceDB accepts only `[a-zA-Z0-9/_|\-=+]`, which
rejects emails and dotted account names. OpenFGA rejects `:`, which rejects URN-style ids. Each
adapter escapes the bytes its engine rejects as `=XX`: `=` followed by two uppercase hex digits of
the UTF-8 byte, as in quoted-printable. `=` itself is escaped, so decoding is exact. Stored ids stay
readable: on OpenFGA, emails and dotted names are unchanged. User ids and roles are lowercased as
Fiat does (`anonymous` maps to Fiat's unrestricted user).

### Where each piece of data is written

| data | authored at | written to the store by |
|---|---|---|
| human memberships | Gate login (SAML, X509, OAuth) → Fiat `/roles` | Fiat's login: the repository's `put` |
| service-account memberships | Front50 service-account save and delete | Front50 event → Fiat; also Fiat's service-account sync |
| application ACLs | Front50 application and application-permission writes | Front50 listeners → Fiat |
| account ACLs | Clouddriver account management (`AccountDefinitionService`) | Clouddriver → Fiat |
| accounts and build services from static config | deploy time | Fiat bootstrap, run by an admin |
| admin, account manager | `fiat.admin.roles`, account-manager roles | not stored; from the user's roles at read time |

Events fire only at writes, once per change, on the replica that served the write.

**Future work: memberships from another service.** Spinnaker need not write memberships at all. A
service outside Spinnaker, such as a directory sync, could load them into the store directly. Fiat
would then skip its login write for those users, and the store would relate principals to groups
from the external data when Fiat reads.

### Write paths

All writes go through one `Reconciler`. It diffs an object's desired tuples against the tuples
stored for it, and applies the difference, so a removed grant is deleted.

**1. Login.** Fiat's existing `RolesController` resolves the user's roles and stores the result in
the `PermissionsRepository`. The store-backed repository's `put` keeps only the roles, and replaces
the user's stored memberships with them. Resolved resources are discarded.

```
Gate login → POST|PUT /roles/{userId}       (RolesController, unchanged)
      ▼
PermissionsResolver.resolve / resolveAndMerge
      ▼
ZanzibarPermissionsRepository.put(permission)          (per-user lock)
      ▼
Reconciler.reconcileUserMemberships       adds = roles − stored; removes = stored − roles
      ▼
ZanzibarStore.apply(adds, removes)
```

The store does not list users, so `POST /roles/sync` refreshes only service accounts and the
unrestricted user. The periodic syncs are off in this mode, whatever else is configured. Logout
keeps memberships until the next login replaces them.

**2. Resource events.** A source sends the changed object to Fiat, in the same JSON Fiat's resource
providers already read from it. Fiat derives the effective permissions with its existing providers,
so derivation rules stay in one place: for applications, the application permission provider, the
execute fallback, and the unknown-application rule; for accounts and build services, their own
providers. Fiat then writes that one object.

```
Front50 application / service-account write, Clouddriver account write
      │  FiatResourceEvents (fiat-api): background, in order; retries; never fails the write
      ▼
POST /zanzibar/resources/{type}   or   POST /zanzibar/resources/{type}/delete
      ▼
ZanzibarResourcePermissions.effective(type, resource)     ← Fiat's own permission providers
      ▼
ResourceChangeNotifier → Reconciler.reconcileResource     (per-object lock, synchronous)
      ▼
ZanzibarStore.apply(adds, removes)
```

Fiat applies each event before replying, so a store failure returns an error and the sender
retries. Front50 events send only the application name, and Front50 re-reads the application's
current state before publishing, so duplicate or out-of-order events reach the same result.

The event hooks:

| source | hook |
|---|---|
| Front50 | an `ApplicationEventListener` and an `ApplicationPermissionEventListener` (post-create, post-update, post-delete); calls in `ServiceAccountsService` |
| Clouddriver | `AccountDefinitionService` create, save, update, and delete. Artifact accounts are skipped; Fiat does not authorize them. |

**3. Bootstrap.** An admin runs it with Fiat's internal `POST /zanzibar/bootstrap`; it never runs at
startup. Fiat reconciles every resource from its providers and every service account from Front50.
This loads a new store, picks up accounts and build services from static configuration, and recovers
any event that was lost after its retries. `?rate=` caps its write requests per second (default
1,000) to regulate load on the store; reads, and the event and login writes that keep the store
current, are not limited. The call returns when the run finishes, with the tuples changed and its
duration, or 409 if a bootstrap is already running on that instance. There is no periodic sweep.

### Read path

`fiat-api` is unchanged. It fetches the user's whole View from Fiat (`GET /authorize/{userId}`),
caches it, and evaluates it in process.

```
service @PreAuthorize → FiatPermissionEvaluator (cached View) → Fiat /authorize/{userId}
      ▼
AuthorizeController (unchanged) → ZanzibarPermissionsRepository.get(userId)
      │  12 concurrent lookupResources (3 types × 4 actions) + the user's memberships
      ▼
CachingZanzibarStore (10 s) → SpiceDB: expand roles and nested groups server-side
```

- **Resource authorizations** ride on one of the user's roles, so the View computes them correctly
  without enumerating thousands of roles. A user without roles reaches only unrestricted resources.
- **Service accounts** come from Fiat's service-account provider with the user's roles, so Fiat's
  existing access rules apply unchanged.
- **Admin and account manager** come from the user's roles and Fiat's configured roles, as in
  Fiat's resolver.
- **Data check.** If the store holds no relationships at all (for example, a wiped datastore), the
  repository reports itself empty and Fiat returns 503, as it does for an empty repository today.
- **View cache.** Each service's `FiatPermissionEvaluator` caches Views per user
  (`services.fiat.cache`; 20 s and 1,000 entries by default). With stock Fiat, a permission change
  waits for the next full sync and then for each service's cached View to expire. With the store,
  a change is readable once written, so the View TTL is the whole delay: an install that raised it
  to spare stock Fiat can lower it again. Each miss then builds a View, 452 ms and 13 store lookups
  on SpiceDB in the benchmark.

### Consistency across replicas

Fiat runs several replicas, and each write lands on one of them. A store's reads use its newest
consistency token, so they see every write up to it, and its writes record their response's token.
One strategy per engine shares the token between replicas, set with
`fiat.zanzibar.<engine>.consistency.strategy`:

| strategy | how | another replica's write is readable |
|---|---|---|
| `none` | no tokens; the engine's default reads | as the engine's default reads allow |
| `local` | each replica's own writes only; for single instances and benchmarks | – |
| `heartbeat` | each replica adds or removes a marker grant every 5 s; each write returns a token newer than every write committed before it | within 5 s |
| `watch` | each replica follows the engine's change stream, reconnecting with backoff | at once |
| `shared-sql` | every second, each replica stores its newest token in a row in Fiat's SQL database if it is newer, then reads the row | within about 2 s |

- A token older than `max-age` (1 minute) is dropped, and reads go without one. `shared-sql` and
  `watch` renew tokens only on writes, so a quiet minute returns reads to the engine's default.
- A strategy's failures are logged and never fail a read; the replica keeps its own writes' token.
- Fiat's caches (10 s for store results, and each service's View cache, 20 s by default) add more
  staleness than any of these strategies.
- **SpiceDB** defaults to `none`: its reads are fully consistent, so nothing needs sharing. With a
  strategy, checks and lookups read at least as fresh as the shared token, which SpiceDB can serve
  from its cache; the reads that compute diffs stay fully consistent. Its tokens are opaque, so
  replicas order them by arrival. Its `watch` needs `track_commit_timestamp=on` in Postgres.
- **OpenFGA** has no consistency tokens.

### Engine recommendation: SpiceDB

Both engines answer point checks in under 1 ms. For the View, SpiceDB is the reliable choice.
OpenFGA's list API cuts results short without an error, at its result cap (1,000) or its list
deadline (3 s). The adapter uses the streamed API, which has no result cap, and fails any listing
that reaches the deadline instead of returning a partial list. It also sets the client's read
timeout from the deadline, since the SDK's 10 s default ended longer listings. With the deadline
raised, OpenFGA returns everything: 32–37× slower than SpiceDB at medium scale, and at full scale a
median View of 499 ms, close to SpiceDB's, but one View in 10 took 28 s, and its login writes and
cached Views were 3–4× slower.

### Dependencies

- A new service, **SpiceDB**, with a logical database on Spinnaker's existing SQL server (see
  [Deployment](#deployment)).
- Client libraries in `kork-zanzibar`: authzed gRPC 1.6.0 and openfga-sdk 0.10.1.
- Changes outside Fiat, each behind `services.fiat.resource-events.enabled`:
  - `fiat-api`: two client methods and `FiatResourceEvents`
  - Front50: three small classes, plus calls in `ServiceAccountsService`
  - Clouddriver: `FiatAccountDefinitionPublisher`, called from `AccountDefinitionService`
- In Fiat, `fiat.zanzibar.enabled=true` turns the periodic role syncs off, overriding any other
  configuration, and defaults the Redis repository off. Fiat's login, resolver, and permission
  providers are reused as they are.
- For a store outside the Fiat pod: a TLS option in the adapters, and credentials for OpenFGA.
- Optional SpiceDB and Postgres components for the kustomize install.

## Deployment

Only Fiat talks to the store; Front50 and Clouddriver send their changes to Fiat. Both engines are
Go services over a SQL datastore, so the store runs beside Fiat on Kubernetes, not inside it. Their
in-memory datastores are for development only.

### Recommended: a standalone SpiceDB service

- **Service.** A SpiceDB service in the Spinnaker namespace with two or more replicas, which share
  work and cache with each other. Only Fiat can reach it.
- **Datastore.** A new logical database on Spinnaker's existing SQL server, created and credentialed
  like each service's own. A dedicated server is the alternative, for isolation or independent
  scaling. Postgres 14 or later is preferred. SpiceDB also runs on MySQL 8.4 or later, but AuthZed
  does not recommend it, and the kustomize install's MySQL 8.0 is too old.
- **Migrations.** Neither engine locks while it migrates, so a job runs each new version's migration
  once, before the rollout.
- **Connections.** TLS and real credentials, or a mesh with mTLS. The adapters gain a TLS option,
  and credentials for OpenFGA.
- **Packaging.** An optional SpiceDB component in the kustomize install, and a new optional Postgres
  component for Clouddriver, Fiat, Front50, Orca and the store. MySQL stays the default, because
  Echo's scheduler and Keel do not support Postgres.
- **Managed upgrades.** Where the cluster allows its cluster-scoped install, the
  [SpiceDB Operator](https://github.com/authzed/spicedb-operator) runs SpiceDB as a cluster resource
  and automates its migrations, upgrades and peer discovery.

### Alternative: a sidecar in the Fiat pod

SpiceDB runs next to Fiat in each pod, reachable only over localhost, with the same datastore and
migration job. It needs no new service and no TLS. In exchange, the store scales and upgrades with
Fiat, and each pod keeps its own cache and database connections. Neither engine documents this
pattern. It suits small installs.

### Other options

- **OpenFGA** runs standalone through its official chart. Its replicas do not share work, and its
  docs favor a few large servers, so a sidecar fits it poorly.
- **Hosted services**, such as AuthZed Cloud or Dedicated for SpiceDB and Auth0 FGA for OpenFGA,
  remove the infrastructure but put an external service on the check path. They need the same TLS
  and credential work.
- **Not viable:** embedding an engine in Fiat, since both are Go only; and one store shared by
  several Spinnaker installs, where one failure would affect them all.

## Drawbacks

- **Slower uncached View.** On SpiceDB an uncached View takes 452 ms p50, versus 37–44 ms to read a
  precomputed one. `fiat-api`'s cache and the store's 10 s cache absorb repeat reads. The trade is
  that nothing is recomputed in the background: no full sync, and no re-resolution on
  service-account changes. Future work would replace Fiat Views with point checks, or proxy View
  requests into point checks.
- **A new service to run:** a distributed, stateful service and its datastore.
- **Events can be lost.** Each source instance publishes in the background, in order, so a Fiat
  outage does not slow the source's writes. An event lost after its retries, or still queued when
  the source restarts, is corrected by the next write to that object, or by the next bootstrap. A
  message broker between the sources and Fiat can mitigate this, if desired.

## Prior Art and Alternatives

### Benchmarked backends

All at the same scale (2,500 users × 5,000 roles from 20,000 groups, 10,000 accounts + 7,000
applications). Full results and commands: `docs/authz-benchmark-comparison.md`.

| backend | uncached View p50 | point check p50 | cost outside the read |
|---|---|---|---|
| **Fiat, Redis repository** | 37 ms (precomputed) | in process | 21–27 s full sync; 22.8 ms per login write |
| **Fiat, Postgres repository** | 44 ms (precomputed) | in process | 27–28 s full sync; 268 ms per login write |
| **Zanzibar, SpiceDB** | **452 ms** (computed) | 0.84 ms | writes at authoring time; 75 ms per login write |
| **Zanzibar, OpenFGA** | 499 ms, complete with a raised list deadline, but one View in 10 took 28 s; truncated by default | 0.79 ms | writes at authoring time; 263 ms per login write |
| **Casbin** (embedded, prototype) | 3.8 s | **4.4 s** | whole policy graph in every replica's heap |

- **Casbin: rejected.** An embedded jCasbin prototype was measured at the same scale. `enforce`
  scans every policy, and role resolution grows with the caller's 5,000 roles; the whole graph must
  also sit in every replica's heap.
- **OpenFGA: supported, not recommended.** See the engine recommendation above.

### Token-based authorization without Fiat (spinnaker/spinnaker#7863)

PR #7863 (draft) takes the opposite approach: it deletes Fiat and makes each service decide locally.

- **Identity.** Gate mints a signed identity token (RS256 JWT, 5-minute lifetime) that carries the
  user's roles and admin flags. Services verify it against Gate's and Front50's published keys.
  Front50 mints run-as tokens for pipelines.
- **ACLs.** Each service owns the ACLs of its own resources. Clouddriver and Orca read application
  ACLs from Front50, one request per application per call.
- **Decisions.** Each service decides in a `PolicyDecisionPointPermissionEvaluator` from the new
  `kork-authz` module.

PR paths: `kork/kork-authz/AUTHZ_MIGRATION.md`, `RoleClaimCodec`, `Front50ResourceAclResolver`,
`RemoteApplicationAclResolver`.

| | #7863 (token-based) | this RFC (store-backed) |
|---|---|---|
| **Fiat** | deleted in one commit (all `fiat/` modules; no fallback mode) | kept; the store is one more `PermissionsRepository`, and the existing ones remain the rollback |
| **`fiat-api` contract** | removed, no compatibility shim | unchanged |
| **Where roles live** | inside every request's identity token | in the store, as memberships; never sent with requests |
| **Role freshness** | Gate's role cache (10 min, reset on each access); roles frozen per pipeline execution | written at login, as today |
| **Where ACLs live** | spread across Front50, Clouddriver, and Igor; application ACLs fetched from Front50 on each request, with no cross-request cache | one store, written when the ACL is authored |
| **Where decisions happen** | each service, in process | Fiat, from the store; each service evaluates a cached View |
| **Service-to-service auth** | added (x509, XFCC header, or Kubernetes service-account token), applied to two endpoints | out of scope |
| **Size of change** | 797 files, +22k / −25k lines, across 13 projects | two new modules, plus small flag-gated hooks in `fiat-api`, Front50, and Clouddriver |

**Token size at these role counts.** #7863 joins all role names into one token claim, compressed
with DEFLATE and then base64url-encoded. Its own test covers 200 roles that share a prefix. With
the benchmark's 7-digit numeric group ids, measured with the same encoding:

| roles | role claim alone |
|---|---|
| 200 | 1.0–1.1 KB |
| ~3,500 (median human) | **16–19 KB** |
| ~5,000 (average) | 24–28 KB |
| ~21,000 (max) | **90–110 KB** |

The whole token is about a third larger again, because the JWT body is base64url-encoded too. At the
median it is already more than twice the 8 KB header limit the PR itself cites, and every service
and proxy in the path would have to raise its header limit. Keeping roles in a store avoids this:
the request carries only the user's identity, and the engine expands the roles server-side.

**What else differs at this scale:**
- **Front50 load and availability.** Per-request application lookups put Front50 on the path of
  most authorized calls in Clouddriver and Orca. A lookup error is handled like an unknown
  application, so with `allowAccessToUnknownApplications` on, a Front50 outage allows access.
- **Revocation.** A removed role stays in effect until the user's cached roles expire (10 minutes
  after last use) and their token expires. Running pipelines keep the roles captured at launch.
- **Reviewability and rollback.** Reviewers asked for a design first and an incremental series: add
  a decision interface with Fiat behind it, then signed tokens, and remove Fiat last. This RFC ships
  dark behind flags, and the existing repositories remain the rollback.

## Known Unknowns

- Behavior of Fiat with SpiceDB in a real deployment, including View latency under concurrent load.
- SpiceDB capacity and tuning: datastore sizing, dispatch concurrency, cache, consistency level.
- Whether either engine works in its own schema inside a shared Postgres database. Neither documents
  it, so the Deployment section uses a separate logical database.
- Which single lookup bounds the View, and how far tuning can reduce it.
- How often events are lost in practice, which determines how often to bootstrap.
- Whether SpiceDB's at-least-as-fresh reads are enough faster than fully consistent ones, at
  Fiat's load, to turn on a consistency strategy.

## Security, Privacy, and Compliance

- **New internal endpoints.** `/zanzibar/resources/*` and `/zanzibar/bootstrap` write authorization
  data. Like `/roles`, they must be reachable only by other Spinnaker services; Gate must never proxy
  them.
- **Derivation stays in Fiat.** Sources send raw objects; Fiat applies its own permission rules, so
  a source cannot grant itself more than those rules allow. Clouddriver sends only an account's name
  and access fields, never its credentials.
- **The store is an authorization datastore** holding the full membership and ACL graph. It must be
  access-controlled like one, and reachable only by Fiat. Connections that leave the Fiat pod need
  TLS, or a mesh with mTLS, and real credentials. A sidecar on localhost needs neither TLS nor a
  mesh, but still a real key. The defaults (plaintext gRPC, a sample key) are for local development
  only.

## Operations

- Run two or more SpiceDB replicas, and monitor them and their database.
- Before rolling out a new engine version, run its migration job once.
- Flags:
  - Fiat: `fiat.zanzibar.enabled`, `fiat.zanzibar.engine` (`spicedb` by default, or `openfga`),
    plus engine connection settings (`fiat.zanzibar.spicedb.*` or `fiat.zanzibar.openfga.*`).
  - Fiat, SpiceDB only: `fiat.zanzibar.spicedb.consistency.strategy` (`none` by default); see
    [Consistency across replicas](#consistency-across-replicas).
  - Front50 and Clouddriver: `services.fiat.resource-events.enabled`.
  - Every service: `services.fiat.cache.expires-after-write-seconds` (20 by default) and
    `services.fiat.cache.max-entries` (1,000), the View cache; see [Read path](#read-path).
- **Bootstrap** never runs on its own. An admin runs `POST /zanzibar/bootstrap?rate=<n>`, with n
  write requests per second, against one Fiat instance after the first deploy, after deploying
  static-config account or build-service changes, and after an outage or a restore. A lower rate
  spreads a large load on a store that is serving traffic.
- Telemetry: View build and lookup latency, event rate and failures, bootstrap duration and changes.
- If OpenFGA is used anyway, set its list deadline and request timeouts well above the slowest
  listing, and set `fiat.zanzibar.openfga.list-deadline` to the same deadline. Listings that reach
  it fail rather than return partial results.

## Risks

- **Operational burden** of a new distributed service. Mitigated by SpiceDB's maturity and a
  flag-gated, reversible rollout.
- **View latency** under load. Mitigated by the two caches; if needed, a point-check mode in
  `fiat-api` removes the View from the request path.
- **Lost events** leave the store stale for one object until its next write or the next bootstrap.
  Mitigated by publisher retries, synchronous apply, and on-demand bootstrap.
- **Bootstrap race.** A bootstrap read can overlap a live event and write older data for that
  object; the next event or bootstrap corrects it.

## Future Possibilities

- **Refresh memberships without a re-login.** This would also benefit Fiat's other repositories.
- **URN-style principals.** Person and group ids in URN form are valid principals; the escaping
  already supports them.
- **Point-check mode in `fiat-api`.** Services would ask Fiat per decision instead of fetching the
  whole View.
- **Signed, identity-only tokens**, as discussed under #7863.
- SpiceDB's Watch API for cross-replica cache invalidation.
- Relationship-based fine-grained authorization (resource hierarchies, inherited permissions).

---

_Benchmark data measured on a single host (a comparison, not tuned-cluster capacity). Reproduce
commands and full results: `docs/authz-benchmark-comparison.md`; engine design:
`kork/kork-zanzibar/README.md`._
