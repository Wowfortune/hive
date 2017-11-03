/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.common.jsonexplain.tez;

public final class Attr implements Comparable<Attr> {
  public final String name;
  public final String value;

  public Attr(String name, String value) {
    super();
    this.name = name;
    this.value = value;
  }

  @Override
  public int compareTo(Attr o) {
    return this.name.compareToIgnoreCase(o.name);
  }

  public String toString() {
    return this.name + this.value;
  }
}
