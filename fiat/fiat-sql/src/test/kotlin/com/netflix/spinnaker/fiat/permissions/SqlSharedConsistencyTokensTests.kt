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

import com.netflix.spinnaker.kork.sql.test.SqlTestUtil
import com.netflix.spinnaker.kork.zanzibar.consistency.ConsistencyToken
import dev.minutest.ContextBuilder
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import java.util.Optional
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.jooq.DSLContext
import org.jooq.SQLDialect
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isTrue

/** The shared consistency token table keeps the newest token per key, on MySQL and Postgres. */
class SqlSharedConsistencyTokensTests : JUnit5Minutests {

    fun ContextBuilder<SqlSharedConsistencyTokens>.offersAndReads(jooq: DSLContext) {
        fixture { SqlSharedConsistencyTokens(jooq) }

        after { jooq.flushAll() }

        test("reads nothing for a key without a token") {
            expectThat(read("titan:none")).isEqualTo(Optional.empty())
        }

        test("stores the first token") {
            offer("titan:t", ConsistencyToken.of("a", 1L))

            expectThat(read("titan:t")).isEqualTo(Optional.of(ConsistencyToken.of("a", 1L)))
        }

        test("replaces an older token") {
            offer("titan:t", ConsistencyToken.of("a", 1L))
            offer("titan:t", ConsistencyToken.of("b", 2L))

            expectThat(read("titan:t")).isEqualTo(Optional.of(ConsistencyToken.of("b", 2L)))
        }

        test("keeps a newer or equal token") {
            offer("titan:t", ConsistencyToken.of("b", 2L))
            offer("titan:t", ConsistencyToken.of("a", 1L))
            offer("titan:t", ConsistencyToken.of("c", 2L))

            expectThat(read("titan:t")).isEqualTo(Optional.of(ConsistencyToken.of("b", 2L)))
        }

        test("keeps one token per key") {
            offer("titan:one", ConsistencyToken.of("a", 1L))
            offer("spicedb:two", ConsistencyToken.of("b", 9L))

            expectThat(read("titan:one")).isEqualTo(Optional.of(ConsistencyToken.of("a", 1L)))
            expectThat(read("spicedb:two")).isEqualTo(Optional.of(ConsistencyToken.of("b", 9L)))
        }

        test("keeps the newest token when replicas offer at once") {
            val executor = Executors.newFixedThreadPool(8)
            try {
                (1L..40L).forEach { order ->
                    executor.execute { offer("titan:race", ConsistencyToken.of("t$order", order)) }
                }
                executor.shutdown()
                expectThat(executor.awaitTermination(1, TimeUnit.MINUTES)).isTrue()
            } finally {
                executor.shutdownNow()
            }

            expectThat(read("titan:race")).isEqualTo(Optional.of(ConsistencyToken.of("t40", 40L)))
        }
    }

    fun tests() = rootContext<SqlSharedConsistencyTokens> {
        context("mysql") {
            val tdb = SqlTestUtil.initTcMysqlDatabase()
            offersAndReads(initDatabase(tdb.dataSource.jdbcUrl, SQLDialect.MYSQL))
        }

        context("postgresql") {
            val tdb = SqlTestUtil.initTcPostgresDatabase()
            offersAndReads(initDatabase(tdb.dataSource.jdbcUrl, SQLDialect.POSTGRES))
        }
    }
}
