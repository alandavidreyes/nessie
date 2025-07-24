/*
 * Copyright (C) 2025 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.projectnessie.versioned.storage.foundationdb;

import org.immutables.value.Value;

import java.time.Duration;

@Value.Immutable
public interface FoundationDBBackendConfig {

  String DEFAULT_CLUSTER_FILE = "/etc/foundationdb/fdb.cluster";

  /** Path to the FoundationDB cluster file. */
  @Value.Default
  default String clusterFile() {
    return FoundationDBLibraryUtils.getClusterFilePath().orElse(DEFAULT_CLUSTER_FILE);
  }

  /** FoundationDB API version. */
  @Value.Default
  default int apiVersion() {
    return 740;
  }

  /** Transaction timeout duration. */
  @Value.Default
  default Duration transactionTimeout() {
    return Duration.ofSeconds(5);
  }

  /** Max connection retries. */
  @Value.Default
  default int maxRetries() {
    return 100;
  }

  /** Key namespace/prefix. */
  @Value.Default
  default String keyPrefix() {
    return "nessie/";
  }

  static ImmutableFoundationDBBackendConfig.Builder builder() {
    return ImmutableFoundationDBBackendConfig.builder();
  }
}
