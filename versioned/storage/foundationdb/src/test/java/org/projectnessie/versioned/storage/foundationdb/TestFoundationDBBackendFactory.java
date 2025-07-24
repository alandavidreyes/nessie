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

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.versioned.storage.common.persist.BackendFactory;
import org.projectnessie.versioned.storage.common.persist.PersistLoader;

class TestFoundationDBBackendFactory {

    private static boolean foundationDBAvailable = false;

    static {
        // Check FoundationDB availability once for the entire test class
        foundationDBAvailable = checkFoundationDBAvailability();
    }

    /** Check if FoundationDB is fully available (cluster file exists and native libraries work). */
    private static boolean checkFoundationDBAvailability() {
        return FoundationDBLibraryUtils.isFoundationDBAvailable();
    }

    @BeforeEach
    void checkFoundationDB() {
        // Skip tests if FoundationDB is not available
        assumeTrue(
            foundationDBAvailable,
            "FoundationDB is not available - skipping tests"
        );
    }

    @Test
    void factoryByName() {
        BackendFactory<FoundationDBBackendConfig> factory =
            PersistLoader.findFactoryByName(FoundationDBBackendFactory.NAME);
        Assertions.assertThat(factory)
            .isNotNull()
            .isInstanceOf(FoundationDBBackendFactory.class);
        Assertions.assertThat(factory.name()).isEqualTo("FoundationDB");
    }

    @Test
    void config() {
        FoundationDBBackendFactory factory = new FoundationDBBackendFactory();
        FoundationDBBackendConfig config = factory.newConfigInstance();
        Assertions.assertThat(config).isNotNull();
        // Config should auto-detect available cluster file or fall back to default
        Assertions.assertThat(config.clusterFile()).isNotNull();
        Assertions.assertThat(config.clusterFile()).endsWith("fdb.cluster");
        Assertions.assertThat(config.apiVersion()).isEqualTo(740);
        Assertions.assertThat(config.keyPrefix()).isEqualTo("nessie/");
    }

    @Test
    void customConfig() {
        FoundationDBBackendConfig config = FoundationDBBackendConfig.builder()
            .keyPrefix("test-nessie/")
            .transactionTimeout(java.time.Duration.ofSeconds(10))
            .maxRetries(50)
            .build();

        Assertions.assertThat(config.keyPrefix()).isEqualTo("test-nessie/");
        Assertions.assertThat(config.transactionTimeout()).isEqualTo(
            java.time.Duration.ofSeconds(10)
        );
        Assertions.assertThat(config.maxRetries()).isEqualTo(50);
    }
}
