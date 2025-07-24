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
package org.projectnessie.quarkus.providers.storage;

import static org.projectnessie.quarkus.config.VersionStoreConfig.VersionStoreType.FOUNDATIONDB;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import org.projectnessie.quarkus.config.QuarkusFoundationDBConfig;
import org.projectnessie.quarkus.providers.versionstore.StoreType;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.foundationdb.FoundationDBBackendConfig;
import org.projectnessie.versioned.storage.foundationdb.FoundationDBBackendFactory;

@StoreType(FOUNDATIONDB)
@Dependent
public class FoundationDBBackendBuilder implements BackendBuilder {

    @Inject
    QuarkusFoundationDBConfig foundationDBConfig;

    @Override
    public Backend buildBackend() {
        FoundationDBBackendFactory factory = new FoundationDBBackendFactory();
        FoundationDBBackendConfig c = FoundationDBBackendConfig.builder()
            .apiVersion(foundationDBConfig.apiVersion())
            .clusterFile(foundationDBConfig.clusterFile())
            .maxRetries(foundationDBConfig.maxRetries())
            .keyPrefix(foundationDBConfig.keyPrefix())
            .build();
        return factory.buildBackend(c);
    }
}
