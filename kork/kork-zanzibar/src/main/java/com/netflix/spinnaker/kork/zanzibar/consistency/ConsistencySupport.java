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

package com.netflix.spinnaker.kork.zanzibar.consistency;

import com.netflix.spinnaker.kork.zanzibar.ZanzibarStore;
import java.util.function.Consumer;
import javax.annotation.Nullable;

/** A store whose engine has consistency tokens; what a {@link ConsistencyStrategy} works with. */
public interface ConsistencySupport extends ZanzibarStore {

  /** This store's tokens: its reads use the current one, and its writes observe theirs. */
  ConsistencyTokens tokens();

  /** Identifies this store's data, so its replicas share one token. */
  String consistencyKey();

  /**
   * Streams the engine's changes after {@code from}, or from now, passing each change's token to
   * {@code onToken}. {@code onEnd} gets the error, or null when the stream completes.
   *
   * @return stops the stream
   */
  Runnable watch(
      @Nullable ConsistencyToken from,
      Consumer<ConsistencyToken> onToken,
      Consumer<Throwable> onEnd);
}
