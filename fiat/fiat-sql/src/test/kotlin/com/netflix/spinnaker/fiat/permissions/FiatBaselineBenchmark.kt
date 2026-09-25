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

package com.netflix.spinnaker.fiat.permissions

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spinnaker.fiat.config.AccountManagerConfig
import com.netflix.spinnaker.fiat.config.FiatAdminConfig
import com.netflix.spinnaker.fiat.config.FiatRoleConfig
import com.netflix.spinnaker.fiat.config.ResourceProvidersHealthIndicator
import com.netflix.spinnaker.fiat.model.resources.Account
import com.netflix.spinnaker.fiat.model.resources.Application
import com.netflix.spinnaker.fiat.model.resources.BuildService
import com.netflix.spinnaker.fiat.model.resources.Resource
import com.netflix.spinnaker.fiat.model.resources.Role
import com.netflix.spinnaker.fiat.model.resources.ServiceAccount
import com.netflix.spinnaker.fiat.providers.BaseResourceProvider
import com.netflix.spinnaker.fiat.providers.BaseServiceAccountResourceProvider
import com.netflix.spinnaker.fiat.providers.DefaultServiceAccountPredicateProvider
import com.netflix.spinnaker.fiat.providers.ResourceProvider
import com.netflix.spinnaker.fiat.roles.Synchronizer
import com.netflix.spinnaker.fiat.roles.UserRolesProvider
import com.netflix.spinnaker.fiat.testing.ProductionShapedFixtures
import com.netflix.spinnaker.kork.dynamicconfig.DynamicConfigService
import com.netflix.spinnaker.kork.dynamicconfig.ScopedCriteria
import com.netflix.spinnaker.kork.jedis.JedisClientDelegate
import com.netflix.spinnaker.kork.sql.config.SqlRetryProperties
import com.netflix.spinnaker.kork.sql.test.SqlTestUtil
import io.github.resilience4j.retry.RetryRegistry
import java.time.Clock
import java.util.Random
import java.util.concurrent.Executors
import kotlin.contracts.ExperimentalContracts
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import redis.clients.jedis.JedisPool

/**
 * Baseline load benchmark for **today's** Fiat, driving the production role-sync code paths against
 * the materialized [PermissionsRepository] on Redis, MySQL, or Postgres:
 *
 *  * **login** — `PUT /roles` (`loginWithRoles`): resolve the user's SAML-asserted roles against
 *    every resource provider, then persist the blob. Split into resolve and persist.
 *  * **full role sync** — the periodic `UserRolesSyncer` job ([Synchronizer.syncAndReturn]): read
 *    every stored user, replay their EXTERNAL roles, re-resolve every user and service account,
 *    then `putAllById`.
 *  * **service-account sync** — `POST /roles/sync/serviceAccount/{id}` (Front50, on save).
 *  * **get + View** — the per-request read every `fiat-api` client makes on a cache miss.
 *
 * Roles come only from login (the role provider returns none). Resources are
 * production-shaped ([ProductionShapedFixtures]): each account and application has its own ACL, so
 * users see different subsets. Resource loading from Front50/Clouddriver is out of scope; the
 * providers serve an in-memory catalog.
 *
 * Expensive and Docker-gated: runs only with `-Dfiat.benchmark=true`. Pick the backend with
 * `-Dfiat.bench.backend=redis|mysql|postgres` (default redis) and dial scale with `fiat.bench.*`;
 * `-Dfiat.bench.heap` sizes the fork:
 *
 * ```
 * ./gradlew :fiat:fiat-sql:test --tests "*FiatBaselineBenchmark" -Dfiat.benchmark=true \
 *   -Dfiat.bench.backend=redis -Dfiat.bench.users=2500 -Dfiat.bench.rolesPerUser=5000
 * ```
 */
@EnabledIfSystemProperty(named = "fiat.benchmark", matches = "true")
@OptIn(ExperimentalContracts::class)
class FiatBaselineBenchmark {

  private val backend = System.getProperty("fiat.bench.backend", "redis")
  private val users: Int = Integer.getInteger("fiat.bench.users", 200)
  private val rolesPerUser: Int = Integer.getInteger("fiat.bench.rolesPerUser", 1000)
  private val groups: Int = Integer.getInteger("fiat.bench.groups", 20000)
  private val accounts: Int = Integer.getInteger("fiat.bench.accounts", 10000)
  private val applications: Int = Integer.getInteger("fiat.bench.applications", 7000)
  private val rolesPerResource: Int = Integer.getInteger("fiat.bench.rolesPerResource", 5)
  private val unrestrictedFraction =
    System.getProperty("fiat.bench.unrestrictedFraction", "0.05").toDouble()
  private val serviceAccounts: Int = Integer.getInteger("fiat.bench.serviceAccounts", 500)
  private val syncRuns: Int = Integer.getInteger("fiat.bench.syncRuns", 2)
  private val serviceAccountSyncSamples: Int =
    Integer.getInteger("fiat.bench.serviceAccountSyncSamples", 10)
  private val getSamples: Int = Integer.getInteger("fiat.bench.getSamples", 500)

  /** SQL repository settings (`permissions-repository.sql.*`). */
  private val sqlAsyncPoolSize: Int = Integer.getInteger("fiat.bench.sqlAsyncPoolSize", 10)
  private val sqlReadBatchSize: Int = Integer.getInteger("fiat.bench.sqlReadBatchSize", 1000)
  private val sqlWriteBatchSize: Int = Integer.getInteger("fiat.bench.sqlWriteBatchSize", 4000)
  private val progressIntervalMs = 5000L

  /** Resource prototypes the repositories read/write per user (every permission-backed type). */
  private val resources: List<Resource> =
    listOf(
      Account(),
      Application(),
      ServiceAccount(),
      Role(),
      BuildService(),
    )

  private val objectMapper = ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL)

  private var redis: GenericContainer<*>? = null
  private var jedisPool: JedisPool? = null
  private var db: SqlTestUtil.TestDatabase? = null
  private var sqlDispatcher: ExecutorCoroutineDispatcher? = null

  @AfterEach
  fun tearDown() {
    sqlDispatcher?.close()
    jedisPool?.close()
    redis?.stop()
    db?.close()
  }

  @Test
  fun benchmark() {
    val repo = buildRepo()
    log("backend: $backend (sql async pool: $sqlAsyncPoolSize, read batch: $sqlReadBatchSize," +
      " write batch: $sqlWriteBatchSize)")
    log(
      "scale: $users users x $rolesPerUser roles from $groups groups; $accounts accounts +" +
        " $applications applications ($rolesPerResource READ + 1 WRITE groups each," +
        " ${Math.round(unrestrictedFraction * 100)}% unrestricted); $serviceAccounts service accounts")

    val roleConfig = FiatRoleConfig()
    val accountProvider = catalog((0 until accounts).map { account(it) }.toSet())
    val applicationProvider = catalog((0 until applications).map { application(it) }.toSet())
    val serviceAccountCatalog = (0 until serviceAccounts).map { serviceAccount(it) }.toSet()
    val serviceAccountProvider =
      object : BaseServiceAccountResourceProvider(listOf(DefaultServiceAccountPredicateProvider(roleConfig))) {
        override fun loadAll(): Set<ServiceAccount> = serviceAccountCatalog
      }
    val providers: List<ResourceProvider<out Resource>> =
      listOf(accountProvider, applicationProvider, serviceAccountProvider)

    val adminConfig = FiatAdminConfig()
    adminConfig.admin.roles = listOf(ProductionShapedFixtures.adminGroupId(groups))
    val resolver =
      DefaultPermissionsResolver(
        noRolesProvider(),
        serviceAccountProvider,
        providers,
        adminConfig,
        AccountManagerConfig(),
        objectMapper,
      )
    val healthIndicator = ResourceProvidersHealthIndicator()
    healthIndicator.setProviders(listOf())
    healthIndicator.setRegistry(NoopRegistry())
    val synchronizer =
      Synchronizer(healthIndicator, resolver, repo, serviceAccountProvider, NoopRegistry(), 10_000L, 600_000L)

    // login: PullBasedUserRolesPublisher.loginWithRoles — resolve, then persist.
    val resolve = LongArray(users)
    val persist = LongArray(users)
    var resourceTotal = 0L
    var start = System.nanoTime()
    var lastLog = System.currentTimeMillis()
    for (i in 0 until users) {
      val externalRoles =
        ProductionShapedFixtures.rolesOf(i, rolesPerUser, groups).map {
          Role(it).setSource(Role.Source.EXTERNAL)
        }
      val user = ExternalUser().setId(ProductionShapedFixtures.userId(i)).setExternalRoles(externalRoles)
      var t = System.nanoTime()
      val permission = resolver.resolveAndMerge(user)
      resolve[i] = System.nanoTime() - t
      t = System.nanoTime()
      repo.put(permission)
      persist[i] = System.nanoTime() - t
      resourceTotal += permission.allResources.size
      val now = System.currentTimeMillis()
      if (now - lastLog >= progressIntervalMs) {
        log("logged in ${i + 1}/$users users...")
        lastLog = now
      }
    }
    var elapsedMs = millisSince(start)
    log("login: $users users in $elapsedMs ms (${perSec(users.toLong(), elapsedMs)} users/s)," +
      " avg ${resourceTotal / users} resources/user")
    report("login: resolve", resolve, "")
    report("login: persist (put)", persist, "")

    // full role sync: the periodic UserRolesSyncer job over every stored user + service account.
    for (run in 1..syncRuns) {
      start = System.nanoTime()
      val synced = synchronizer.syncAndReturn(listOf())
      elapsedMs = millisSince(start)
      log("full role sync run $run: $synced principals in $elapsedMs ms" +
        " (${perSec(synced, elapsedMs)} principals/s)")
    }

    // service-account sync: Front50's POST /roles/sync/serviceAccount/{id} on save.
    if (serviceAccounts > 0 && serviceAccountSyncSamples > 0) {
      val saSync = LongArray(serviceAccountSyncSamples)
      var usersResynced = 0L
      for (k in 0 until serviceAccountSyncSamples) {
        val i = k % serviceAccounts
        val t = System.nanoTime()
        usersResynced +=
          synchronizer.syncServiceAccount(
            ProductionShapedFixtures.serviceAccountName(i),
            ProductionShapedFixtures.serviceAccountMemberOf(i, groups))
        saSync[k] = System.nanoTime() - t
      }
      report("service-account sync", saSync, "avg ${usersResynced / serviceAccountSyncSamples} users re-resolved")
    }

    // get + View: the per-request read every fiat-api client makes on a cache miss.
    val get = LongArray(getSamples)
    val rnd = Random(7)
    var roleTotal = 0L
    var viewResources = 0L
    lastLog = System.currentTimeMillis()
    for (i in 0 until getSamples) {
      val id = ProductionShapedFixtures.userId(rnd.nextInt(users))
      val t = System.nanoTime()
      val permission = repo.get(id).orElseThrow()
      val view = permission.view
      get[i] = System.nanoTime() - t
      roleTotal += permission.roles.size.toLong()
      viewResources += (view.accounts.size + view.applications.size).toLong()
      val now = System.currentTimeMillis()
      if (now - lastLog >= progressIntervalMs) {
        log("get ${i + 1}/$getSamples...")
        lastLog = now
      }
    }
    report(
      "get + View",
      get,
      "avg ${roleTotal / getSamples} roles, ${viewResources / getSamples} accounts+apps per View")
  }

  private fun account(i: Int): Account {
    val name = ProductionShapedFixtures.accountName(i)
    val account = Account()
    account.setName(name)
    account.setCloudProvider("kubernetes")
    account.setPermissions(
      ProductionShapedFixtures.permissionsOf(name, rolesPerResource, groups, unrestrictedFraction))
    return account
  }

  private fun application(i: Int): Application {
    val name = ProductionShapedFixtures.applicationName(i)
    val application = Application()
    application.setName(name)
    application.setPermissions(
      ProductionShapedFixtures.permissionsOf(name, rolesPerResource, groups, unrestrictedFraction))
    return application
  }

  private fun serviceAccount(i: Int): ServiceAccount {
    val serviceAccount = ServiceAccount()
    serviceAccount.setName(ProductionShapedFixtures.serviceAccountName(i))
    serviceAccount.setMemberOf(ProductionShapedFixtures.serviceAccountMemberOf(i, groups))
    return serviceAccount
  }

  private fun <R : Resource> catalog(items: Set<R>): BaseResourceProvider<R> =
    object : BaseResourceProvider<R>() {
      override fun loadAll(): Set<R> = items
    }

  /** Returns no roles, so every role comes from login. */
  private fun noRolesProvider() =
    object : UserRolesProvider {
      override fun loadRoles(user: ExternalUser): List<Role> = listOf()

      override fun multiLoadRoles(users: Collection<ExternalUser>): Map<String, Collection<Role>> =
        HashMap()
    }

  private fun buildRepo(): PermissionsRepository =
    when (backend) {
      "redis" -> {
        val container =
          GenericContainer(DockerImageName.parse("library/redis:5-alpine")).withExposedPorts(6379)
        container.start()
        redis = container
        val pool = JedisPool(container.host, container.getMappedPort(6379))
        jedisPool = pool
        val configProps = RedisPermissionRepositoryConfigProps().apply { prefix = "fiatbench" }
        RedisPermissionsRepository(
          objectMapper,
          JedisClientDelegate(pool),
          resources,
          configProps,
          RetryRegistry.ofDefaults(),
        )
      }
      "mysql",
      "postgres" -> {
        val testDb =
          if (backend == "postgres") SqlTestUtil.initTcPostgresDatabase()
          else SqlTestUtil.initTcMysqlDatabase()
        db = testDb
        val dispatcher =
          if (sqlAsyncPoolSize > 0) {
            Executors.newFixedThreadPool(sqlAsyncPoolSize).asCoroutineDispatcher()
          } else {
            null
          }
        sqlDispatcher = dispatcher
        SqlPermissionsRepository(
          Clock.systemUTC(),
          objectMapper,
          testDb.context,
          SqlRetryProperties(),
          resources,
          dispatcher,
          sqlConfig(),
        )
      }
      else -> throw IllegalArgumentException("unknown fiat.bench.backend: $backend (redis|mysql|postgres)")
    }

  /** Serves the SQL repository's batch-size settings; everything else takes its default. */
  private fun sqlConfig(): DynamicConfigService {
    val values =
      mapOf(
        "permissions-repository.sql.read-batch-size" to sqlReadBatchSize,
        "permissions-repository.sql.write-batch-size" to sqlWriteBatchSize,
      )
    return object : DynamicConfigService {
      @Suppress("UNCHECKED_CAST")
      override fun <T : Any> getConfig(configType: Class<T>, configName: String, defaultValue: T): T =
        (values[configName] as T?) ?: defaultValue

      override fun isEnabled(flagName: String, defaultValue: Boolean) = defaultValue

      override fun isEnabled(flagName: String, defaultValue: Boolean, criteria: ScopedCriteria) =
        defaultValue
    }
  }

  private fun report(name: String, nanos: LongArray, note: String) {
    nanos.sort()
    log(
      "$name: p50=${ms(percentile(nanos, 50))} p95=${ms(percentile(nanos, 95))}" +
        " p99=${ms(percentile(nanos, 99))} max=${ms(nanos.last())} ms (n=${nanos.size}) $note")
  }

  private fun percentile(sorted: LongArray, p: Int): Long {
    val idx = Math.ceil(p / 100.0 * sorted.size).toInt() - 1
    return sorted[idx.coerceIn(0, sorted.size - 1)]
  }

  private fun millisSince(startNanos: Long) = (System.nanoTime() - startNanos) / 1_000_000

  private fun perSec(count: Long, millis: Long) = if (millis == 0L) count else count * 1000 / millis

  private fun ms(nanos: Long) = String.format("%.2f", nanos / 1_000_000.0)

  private fun log(message: String) = println("[fiat-bench] $message")
}
