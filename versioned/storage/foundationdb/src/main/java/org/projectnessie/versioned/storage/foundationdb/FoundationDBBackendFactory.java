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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.tuple.Tuple;
import jakarta.annotation.Nonnull;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Set;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.BackendFactory;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;

public class FoundationDBBackendFactory
  implements BackendFactory<FoundationDBBackendConfig> {

  public static final String NAME = "FoundationDB";

  @Override
  @Nonnull
  public String name() {
    return NAME;
  }

  @Override
  @Nonnull
  public FoundationDBBackendConfig newConfigInstance() {
    return FoundationDBBackendConfig.builder().build();
  }

  @Override
  @Nonnull
  public Backend buildBackend(@Nonnull FoundationDBBackendConfig config) {
    return new FoundationDBBackend(config);
  }

  static class FoundationDBBackend implements Backend {

    private final FoundationDBBackendConfig config;
    private final Database database;

    public FoundationDBBackend(FoundationDBBackendConfig config) {
      this.config = config;

      // Configure native library if not already set
      if (System.getProperty("FDB_LIBRARY_PATH_FDB_C") == null) {
        if (!FoundationDBLibraryUtils.configureNativeLibrary()) {
          throw new RuntimeException(
            "FoundationDB native library not found. Platform info:\n" +
            FoundationDBLibraryUtils.getPlatformInfo()
          );
        }
      }

      // Initialize FoundationDB
      FDB fdb = FDB.selectAPIVersion(config.apiVersion());

      // Check if cluster file exists
      String clusterFile = config.clusterFile();
      if (!Files.exists(Paths.get(clusterFile))) {
        throw new RuntimeException(
          "FoundationDB cluster file not found: " + clusterFile
        );
      }

      this.database = fdb.open(clusterFile);
    }

    @Override
    public void close() {
      if (database != null) {
        database.close();
      }
    }

    @Override
    @Nonnull
    public Optional<String> setupSchema() {
      // FoundationDB doesn't require explicit schema setup
      // But we can verify connectivity and return cluster info
      try {
        database.run(tr -> {
          // Simple connectivity test by reading a dummy key
          byte[] testKey = Tuple.from(config.keyPrefix() + "test").pack();
          tr.get(testKey).join();
          return null;
        });
        return Optional.of("FoundationDB cluster at " + config.clusterFile());
      } catch (Exception e) {
        throw new RuntimeException(
          "Failed to connect to FoundationDB cluster",
          e
        );
      }
    }

    @Override
    @Nonnull
    public PersistFactory createFactory() {
      return new FoundationDBPersistFactory(database, config);
    }

    @Override
    public void eraseRepositories(Set<String> repositoryIds) {
      database.run(tr -> {
        for (String repositoryId : repositoryIds) {
          // Clear all keys with repository prefix
          String repoPrefix = config.keyPrefix() + "repo:" + repositoryId + ":";
          byte[] startKey = Tuple.from(repoPrefix).pack();
          byte[] endKey = Tuple.from(repoPrefix + "\uFFFF").pack();
          tr.clear(startKey, endKey);
        }
        return null;
      });
    }
  }

  static class FoundationDBPersistFactory implements PersistFactory {

    private final Database database;
    private final FoundationDBBackendConfig config;

    FoundationDBPersistFactory(Database database, FoundationDBBackendConfig config) {
      this.database = database;
      this.config = config;
    }

    @Override
    @Nonnull
    public Persist newPersist(@Nonnull StoreConfig storeConfig) {
      return new FoundationDBPersist(database, config, storeConfig);
    }
  }
}
