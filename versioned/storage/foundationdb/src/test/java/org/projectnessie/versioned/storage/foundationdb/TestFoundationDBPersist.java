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
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.newCommitHeaders;
import static org.projectnessie.versioned.storage.common.objtypes.CommitObj.commitBuilder;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;
import static org.projectnessie.versioned.storage.common.persist.Reference.reference;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Set;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitType;
import org.projectnessie.versioned.storage.common.objtypes.StandardObjType;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.CloseableIterator;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;
import org.projectnessie.versioned.storage.common.persist.Reference;

class TestFoundationDBPersist {

    private Backend backend;
    private Persist persist;
    private static boolean foundationDBAvailable = false;

    static {
        // Check FoundationDB availability once for the entire test class
        foundationDBAvailable = checkFoundationDBAvailability();
    }

    /** Check if FoundationDB is fully available (cluster file exists and native libraries work). */
    private static boolean checkFoundationDBAvailability() {
        return FoundationDBLibraryUtils.isFoundationDBAvailable();
    }

    /**
     * Check if FoundationDB is available by looking for cluster files in common locations. This is a
     * simple check - if the cluster file exists, we assume FoundationDB might be available. The
     * actual connectivity will be tested when the backend is created.
     */
    private static boolean isFoundationDBAvailable() {
        String[] possibleClusterFiles = {
            "/etc/foundationdb/fdb.cluster",
            "/usr/local/etc/foundationdb/fdb.cluster",
        };

        for (String clusterFile : possibleClusterFiles) {
            if (Files.exists(Paths.get(clusterFile))) {
                return true;
            }
        }
        return false;
    }

    /** Get the first available FoundationDB cluster file path. */
    private static String getFoundationDBClusterFile() {
        String[] possibleClusterFiles = {
            "/etc/foundationdb/fdb.cluster",
            "/usr/local/etc/foundationdb/fdb.cluster",
        };

        for (String clusterFile : possibleClusterFiles) {
            if (Files.exists(Paths.get(clusterFile))) {
                return clusterFile;
            }
        }
        throw new RuntimeException("No FoundationDB cluster file found");
    }

    @BeforeEach
    void setup() throws Exception {
        // Skip tests if FoundationDB is not available
        assumeTrue(
            foundationDBAvailable,
            "FoundationDB is not available - skipping tests"
        );

        FoundationDBBackendFactory factory = new FoundationDBBackendFactory();
        FoundationDBBackendConfig config = FoundationDBBackendConfig.builder()
            .keyPrefix("test-" + System.currentTimeMillis() + "/")
            .build();

        backend = factory.buildBackend(config);
        backend.setupSchema();

        PersistFactory persistFactory = backend.createFactory();
        persist = persistFactory.newPersist(StoreConfig.Adjustable.empty());
    }

    @AfterEach
    void cleanup() throws Exception {
        if (persist != null) {
            persist.erase();
        }
        if (backend != null) {
            backend.close();
        }
    }

    @Test
    void referenceOperations() throws Exception {
        ObjId pointer1 = randomObjId();
        ObjId pointer2 = randomObjId();

        String refName = "refs/heads/main";
        Reference ref1 = reference(
            refName,
            pointer1,
            false,
            Instant.now().toEpochMilli(),
            null
        );

        // Test addReference
        Reference added = persist.addReference(ref1);
        Assertions.assertThat(added).isEqualTo(ref1);

        // Test fetchReference
        Reference fetched = persist.fetchReference(refName);
        Assertions.assertThat(fetched).isEqualTo(ref1);

        // Test adding duplicate reference fails
        Assertions.assertThatThrownBy(() ->
            persist.addReference(ref1)
        ).isInstanceOf(RefAlreadyExistsException.class);

        // Test updateReferencePointer
        Reference updated = persist.updateReferencePointer(ref1, pointer2);
        Assertions.assertThat(updated.pointer()).isEqualTo(pointer2);
        Assertions.assertThat(updated.name()).isEqualTo(refName);

        // Test fetchReferences with multiple names
        Reference[] refs = persist.fetchReferences(
            new String[] { refName, "non-existent" }
        );
        Assertions.assertThat(refs).hasSize(2);
        Assertions.assertThat(refs[0].pointer()).isEqualTo(pointer2);
        Assertions.assertThat(refs[1]).isNull();

        // Test markReferenceAsDeleted
        Reference deleted = persist.markReferenceAsDeleted(updated);
        Assertions.assertThat(deleted.deleted()).isTrue();
        Assertions.assertThat(deleted.pointer()).isEqualTo(pointer2);

        // Test purgeReference
        persist.purgeReference(deleted);
        Assertions.assertThat(persist.fetchReference(refName)).isNull();
    }

    @Test
    void referenceConditions() throws Exception {
        ObjId pointer1 = randomObjId();
        ObjId pointer2 = randomObjId();

        String refName = "refs/heads/test";
        Reference ref1 = reference(
            refName,
            pointer1,
            false,
            Instant.now().toEpochMilli(),
            null
        );

        persist.addReference(ref1);

        // Test update with wrong pointer fails
        Reference wrongRef = reference(
            refName,
            pointer2,
            false,
            Instant.now().toEpochMilli(),
            null
        );
        Assertions.assertThatThrownBy(() ->
            persist.updateReferencePointer(wrongRef, randomObjId())
        ).isInstanceOf(RefConditionFailedException.class);

        // Test mark as deleted with wrong pointer fails
        Assertions.assertThatThrownBy(() ->
            persist.markReferenceAsDeleted(wrongRef)
        ).isInstanceOf(RefConditionFailedException.class);

        // Test operations on non-existent reference
        Reference nonExistent = reference(
            "non-existent",
            pointer1,
            false,
            Instant.now().toEpochMilli(),
            null
        );
        Assertions.assertThatThrownBy(() ->
            persist.updateReferencePointer(nonExistent, pointer2)
        ).isInstanceOf(RefNotFoundException.class);
        Assertions.assertThatThrownBy(() ->
            persist.markReferenceAsDeleted(nonExistent)
        ).isInstanceOf(RefNotFoundException.class);
        Assertions.assertThatThrownBy(() ->
            persist.purgeReference(nonExistent)
        ).isInstanceOf(RefNotFoundException.class);
    }

    @Test
    void objectOperations() throws Exception {
        ObjId id1 = randomObjId();
        ObjId id2 = randomObjId();

        CommitObj obj1 = commitBuilder()
            .id(id1)
            .created(Instant.now().toEpochMilli() * 1000)
            .seq(1L)
            .commitType(CommitType.NORMAL)
            .headers(newCommitHeaders().build())
            .message("Test commit 1")
            .incrementalIndex(ByteString.EMPTY)
            .build();

        CommitObj obj2 = commitBuilder()
            .id(id2)
            .created(Instant.now().toEpochMilli() * 1000)
            .seq(2L)
            .commitType(CommitType.NORMAL)
            .headers(newCommitHeaders().build())
            .message("Test commit 2")
            .incrementalIndex(ByteString.EMPTY)
            .build();

        // Test storeObj
        boolean stored1 = persist.storeObj(obj1);
        Assertions.assertThat(stored1).isTrue();

        // Test storing same object again returns false
        boolean stored1Again = persist.storeObj(obj1);
        Assertions.assertThat(stored1Again).isFalse();

        // Test fetchTypedObjsIfExist
        Obj[] objs = persist.fetchTypedObjsIfExist(
            new ObjId[] { id1, id2, null },
            StandardObjType.COMMIT,
            CommitObj.class
        );
        Assertions.assertThat(objs).hasSize(3);
        Assertions.assertThat(objs[0]).isEqualTo(obj1);
        Assertions.assertThat(objs[1]).isNull(); // obj2 not stored yet
        Assertions.assertThat(objs[2]).isNull(); // null input

        // Test storeObjs
        boolean[] storedResults = persist.storeObjs(
            new Obj[] { obj2, obj1, null }
        );
        Assertions.assertThat(storedResults).containsExactly(
            true,
            false,
            false
        );

        // Verify both objects are now stored
        objs = persist.fetchTypedObjsIfExist(
            new ObjId[] { id1, id2 },
            StandardObjType.COMMIT,
            CommitObj.class
        );
        Assertions.assertThat(objs).hasSize(2);
        Assertions.assertThat(objs[0]).isEqualTo(obj1);
        Assertions.assertThat(objs[1]).isEqualTo(obj2);

        // Test deleteObj
        persist.deleteObj(id1);
        Obj fetched = persist.fetchTypedObjsIfExist(
            new ObjId[] { id1 },
            null,
            Obj.class
        )[0];
        Assertions.assertThat(fetched).isNull();

        // Test deleteObjs
        persist.deleteObjs(new ObjId[] { id2, null });
        fetched = persist.fetchTypedObjsIfExist(
            new ObjId[] { id2 },
            null,
            Obj.class
        )[0];
        Assertions.assertThat(fetched).isNull();
    }

    @Test
    void scanAllObjects() throws Exception {
        ObjId id1 = randomObjId();
        ObjId id2 = randomObjId();

        CommitObj obj1 = commitBuilder()
            .id(id1)
            .created(Instant.now().toEpochMilli() * 1000)
            .seq(1L)
            .commitType(CommitType.NORMAL)
            .headers(newCommitHeaders().build())
            .message("Test commit 1")
            .incrementalIndex(ByteString.EMPTY)
            .build();

        CommitObj obj2 = commitBuilder()
            .id(id2)
            .created(Instant.now().toEpochMilli() * 1000)
            .seq(2L)
            .commitType(CommitType.NORMAL)
            .headers(newCommitHeaders().build())
            .message("Test commit 2")
            .incrementalIndex(ByteString.EMPTY)
            .build();

        persist.storeObj(obj1);
        persist.storeObj(obj2);

        // Test scanning all objects
        try (
            CloseableIterator<Obj> iterator = persist.scanAllObjects(Set.of());
        ) {
            int count = 0;
            while (iterator.hasNext()) {
                Obj obj = iterator.next();
                Assertions.assertThat(obj).isNotNull();
                Assertions.assertThat(obj.type()).isEqualTo(
                    StandardObjType.COMMIT
                );
                count++;
            }

            Assertions.assertThat(count).isEqualTo(2);
        }

        // Test scanning with type filter
        try (
            CloseableIterator<Obj> iterator = persist.scanAllObjects(
                Set.of(StandardObjType.COMMIT)
            );
        ) {
            int count = 0;
            while (iterator.hasNext()) {
                Obj obj = iterator.next();
                Assertions.assertThat(obj).isNotNull();
                Assertions.assertThat(obj.type()).isEqualTo(
                    StandardObjType.COMMIT
                );
                count++;
            }
            Assertions.assertThat(count).isEqualTo(2);
        }

        // Test scanning with non-matching type filter
        try (
            CloseableIterator<Obj> iterator = persist.scanAllObjects(
                Set.of(StandardObjType.VALUE)
            );
        ) {
            Assertions.assertThat(iterator.hasNext()).isFalse();
        }
    }

    @Test
    void upsertOperations() throws Exception {
        ObjId id1 = randomObjId();

        CommitObj obj1 = commitBuilder()
            .id(id1)
            .created(Instant.now().toEpochMilli() * 1000)
            .seq(1L)
            .commitType(CommitType.NORMAL)
            .headers(newCommitHeaders().build())
            .message("Original message")
            .incrementalIndex(ByteString.EMPTY)
            .build();

        CommitObj obj1Updated = commitBuilder()
            .id(id1)
            .created(Instant.now().toEpochMilli() * 1000)
            .seq(1L)
            .commitType(CommitType.NORMAL)
            .headers(newCommitHeaders().build())
            .message("Updated message")
            .incrementalIndex(ByteString.EMPTY)
            .build();

        // Test upsert on non-existing object
        persist.upsertObj(obj1);
        Obj stored = persist.fetchTypedObjsIfExist(
            new ObjId[] { id1 },
            null,
            Obj.class
        )[0];
        Assertions.assertThat(stored).isEqualTo(obj1);

        // Test upsert on existing object (should update)
        persist.upsertObj(obj1Updated);
        stored = persist.fetchTypedObjsIfExist(
            new ObjId[] { id1 },
            null,
            Obj.class
        )[0];
        Assertions.assertThat(stored).isEqualTo(obj1Updated);

        // Test upsertObjs
        ObjId id2 = randomObjId();
        CommitObj obj2 = commitBuilder()
            .id(id2)
            .created(Instant.now().toEpochMilli() * 1000)
            .seq(2L)
            .commitType(CommitType.NORMAL)
            .headers(newCommitHeaders().build())
            .message("Test commit 2")
            .incrementalIndex(ByteString.EMPTY)
            .build();

        persist.upsertObjs(new Obj[] { obj1, obj2, null });

        Obj[] objs = persist.fetchTypedObjsIfExist(
            new ObjId[] { id1, id2 },
            null,
            Obj.class
        );
        Assertions.assertThat(objs[0]).isEqualTo(obj1);
        Assertions.assertThat(objs[1]).isEqualTo(obj2);
    }

    @Test
    void eraseRepository() throws Exception {
        // Add some data
        ObjId pointer = randomObjId();
        Reference ref = reference(
            "refs/heads/main",
            pointer,
            false,
            Instant.now().toEpochMilli(),
            null
        );
        persist.addReference(ref);

        CommitObj obj = commitBuilder()
            .id(randomObjId())
            .created(Instant.now().toEpochMilli() * 1000)
            .seq(1L)
            .commitType(CommitType.NORMAL)
            .headers(newCommitHeaders().build())
            .message("Test commit")
            .incrementalIndex(ByteString.EMPTY)
            .build();
        persist.storeObj(obj);

        // Verify data exists
        Assertions.assertThat(
            persist.fetchReference("refs/heads/main")
        ).isNotNull();
        Assertions.assertThat(
            persist.fetchTypedObjsIfExist(
                new ObjId[] { obj.id() },
                null,
                Obj.class
            )[0]
        ).isNotNull();

        // Erase repository
        persist.erase();

        // Verify data is gone
        Assertions.assertThat(
            persist.fetchReference("refs/heads/main")
        ).isNull();
        Assertions.assertThat(
            persist.fetchTypedObjsIfExist(
                new ObjId[] { obj.id() },
                null,
                Obj.class
            )[0]
        ).isNull();
    }
}
