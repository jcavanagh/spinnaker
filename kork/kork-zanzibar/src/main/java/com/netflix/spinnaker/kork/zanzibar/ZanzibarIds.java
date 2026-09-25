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

package com.netflix.spinnaker.kork.zanzibar;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.function.IntPredicate;

/**
 * Reversible escaping of ids into an engine's id charset. Rejected bytes, and the escape character
 * {@code =}, become {@code =XX} (uppercase hex of the UTF-8 byte). The wildcard {@code *} passes
 * through.
 */
public final class ZanzibarIds {

  private static final char[] HEX = "0123456789ABCDEF".toCharArray();

  private ZanzibarIds() {}

  /** Escape {@code id}; {@code allowed} is tested only for ASCII bytes. */
  public static String encode(String id, IntPredicate allowed) {
    if ("*".equals(id)) {
      return id;
    }
    var bytes = id.getBytes(StandardCharsets.UTF_8);
    var sb = new StringBuilder(bytes.length);
    for (var b : bytes) {
      var c = b & 0xFF;
      if (c != '=' && c < 0x80 && allowed.test(c)) {
        sb.append((char) c);
      } else {
        sb.append('=').append(HEX[c >> 4]).append(HEX[c & 0xF]);
      }
    }
    return sb.toString();
  }

  public static String decode(String id) {
    if (id.indexOf('=') < 0) {
      return id;
    }
    var out = new ByteArrayOutputStream(id.length());
    for (var i = 0; i < id.length(); i++) {
      var c = id.charAt(i);
      if (c == '=') {
        out.write(Integer.parseInt(id, i + 1, i + 3, 16));
        i += 2;
      } else {
        out.write(c);
      }
    }
    return out.toString(StandardCharsets.UTF_8);
  }
}
