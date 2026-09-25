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

/** A point in a store's history; a read at a token sees every write up to it. */
public interface ConsistencyToken {

  /** The engine's encoded token. */
  String value();

  /** Orders one store's tokens; higher is newer. */
  long order();

  static ConsistencyToken of(String value, long order) {
    return new Token(value, order);
  }

  record Token(String value, long order) implements ConsistencyToken {}
}
