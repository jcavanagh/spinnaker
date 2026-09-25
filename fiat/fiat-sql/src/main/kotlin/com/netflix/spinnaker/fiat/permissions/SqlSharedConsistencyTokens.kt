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

import com.netflix.spinnaker.kork.zanzibar.consistency.ConsistencyToken
import com.netflix.spinnaker.kork.zanzibar.consistency.SharedConsistencyTokens
import java.util.Optional
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

/** Each Zanzibar store's newest consistency token, shared by Fiat's replicas. */
class SqlSharedConsistencyTokens(private val jooq: DSLContext) : SharedConsistencyTokens {

    override fun offer(key: String, token: ConsistencyToken) {
        if (update(key, token) > 0) {
            return
        }
        val inserted = jooq.insertInto(TABLE, KEY, TOKEN, ORDER, UPDATED_AT)
            .values(key, token.value(), token.order(), System.currentTimeMillis())
            .onDuplicateKeyIgnore()
            .execute()
        if (inserted == 0) {
            // Another replica inserted first; keep the newer token.
            update(key, token)
        }
    }

    override fun read(key: String): Optional<ConsistencyToken> =
        Optional.ofNullable(
            jooq.select(TOKEN, ORDER)
                .from(TABLE)
                .where(KEY.eq(key))
                .fetchOne()
                ?.let { ConsistencyToken.of(it.value1(), it.value2()) }
        )

    private fun update(key: String, token: ConsistencyToken): Int =
        jooq.update(TABLE)
            .set(TOKEN, token.value())
            .set(ORDER, token.order())
            .set(UPDATED_AT, System.currentTimeMillis())
            .where(KEY.eq(key).and(ORDER.lt(token.order())))
            .execute()

    private companion object {
        val TABLE = DSL.table(DSL.name("fiat_zanzibar_consistency_token"))
        val KEY = DSL.field(DSL.name("store_key"), SQLDataType.VARCHAR(255))
        val TOKEN = DSL.field(DSL.name("token"), SQLDataType.VARCHAR(1024))
        val ORDER = DSL.field(DSL.name("token_order"), SQLDataType.BIGINT)
        val UPDATED_AT = DSL.field(DSL.name("updated_at"), SQLDataType.BIGINT)
    }
}
