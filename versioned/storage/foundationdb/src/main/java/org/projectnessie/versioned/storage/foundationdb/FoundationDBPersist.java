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

import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializeObj;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializeReference;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializeObj;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializeReference;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Tuple;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.objtypes.UpdateableObj;
import org.projectnessie.versioned.storage.common.persist.CloseableIterator;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;

public class FoundationDBPersist implements Persist {

  private static final int RETRY_DELAY_MILLIS = 100;

  private final Database database;
  private final FoundationDBBackendConfig config;
  private final StoreConfig storeConfig;
  private final String keyPrefix;

  public FoundationDBPersist(
    Database database,
    FoundationDBBackendConfig config,
    StoreConfig storeConfig
  ) {
    this.database = database;
    this.config = config;
    this.storeConfig = storeConfig;
    this.keyPrefix =
      config.keyPrefix() + "repo:" + storeConfig.repositoryId() + ":";
  }

  @Override
  @Nonnull
  public String name() {
    return FoundationDBBackendFactory.NAME;
  }

  @Override
  @Nonnull
  public StoreConfig config() {
    return storeConfig;
  }

  // Reference operations

  @Override
  @Nullable
  public Reference fetchReference(@Nonnull String name) {
    return database.run(tr -> {
      byte[] key = referenceKey(name);
      byte[] value = tr.get(key).join();

      if (value == null) {
        return null;
      }

      try {
        return deserializeReference(value);
      } catch (Exception e) {
        throw new RuntimeException("Failed to parse reference: " + name, e);
      }
    });
  }

  @Override
  @Nonnull
  public Reference[] fetchReferences(@Nonnull String[] names) {
    return database.run(tr -> {
      Reference[] results = new Reference[names.length];
      List<CompletableFuture<byte[]>> futures = new ArrayList<>();

      for (String name : names) {
        byte[] key = referenceKey(name);
        futures.add(tr.get(key));
      }

      try {
        for (int i = 0; i < names.length; i++) {
          byte[] value = futures.get(i).get();
          if (value != null) {
            results[i] = deserializeReference(value);
          }
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException("Failed to fetch references", e);
      }

      return results;
    });
  }

  @Override
  @Nonnull
  public Reference addReference(@Nonnull Reference reference)
    throws RefAlreadyExistsException {
    try {
      return database.run(tr -> {
        byte[] key = referenceKey(reference.name());

        // Check if reference already exists
        byte[] existing = tr.get(key).join();
        if (existing != null) {
          throw new RuntimeException(new RefAlreadyExistsException(reference));
        }

        // Add new reference
        byte[] serialized = serializeReference(reference);
        tr.set(key, serialized);
        return reference;
      });
    } catch (Exception e) {
      if (e instanceof RefAlreadyExistsException) {
        throw (RefAlreadyExistsException) e;
      }
      // Unwrap CompletionException to find the original cause
      Throwable cause = e.getCause();
      while (cause != null) {
        if (cause instanceof RefAlreadyExistsException) {
          throw (RefAlreadyExistsException) cause;
        }
        if (
          cause instanceof RuntimeException &&
          cause.getCause() instanceof RefAlreadyExistsException
        ) {
          throw (RefAlreadyExistsException) cause.getCause();
        }
        cause = cause.getCause();
      }
      throw new RuntimeException(
        "Failed to add reference: " + reference.name(),
        e
      );
    }
  }

  @Override
  @Nonnull
  public Reference markReferenceAsDeleted(@Nonnull Reference reference)
    throws RefNotFoundException, RefConditionFailedException {
    try {
      return database.run(tr -> {
        byte[] key = referenceKey(reference.name());

        // Get current reference
        byte[] current = tr.get(key).join();
        if (current == null) {
          throw new RuntimeException(new RefNotFoundException(reference));
        }

        try {
          Reference currentRef = deserializeReference(current);

          // Verify current reference matches expected
          if (currentRef.deleted()) {
            throw new RuntimeException(
              new RefConditionFailedException(reference)
            );
          }
          if (!currentRef.pointer().equals(reference.pointer())) {
            throw new RuntimeException(
              new RefConditionFailedException(reference)
            );
          }

          // Mark as deleted
          Reference deleted = Reference.reference(
            reference.name(),
            reference.pointer(),
            true,
            currentRef.createdAtMicros(),
            currentRef.extendedInfoObj()
          );

          byte[] serialized = serializeReference(deleted);
          tr.set(key, serialized);
          return deleted;
        } catch (Exception e) {
          if (
            e instanceof RefConditionFailedException ||
            e instanceof RefNotFoundException
          ) {
            throw new RuntimeException(e);
          }
          throw new RuntimeException("Failed to mark reference as deleted", e);
        }
      });
    } catch (Exception e) {
      if (e instanceof RefNotFoundException) {
        throw (RefNotFoundException) e;
      }
      if (e instanceof RefConditionFailedException) {
        throw (RefConditionFailedException) e;
      }
      // Unwrap nested exceptions
      Throwable cause = e.getCause();
      while (cause != null) {
        if (cause instanceof RefNotFoundException) {
          throw (RefNotFoundException) cause;
        }
        if (cause instanceof RefConditionFailedException) {
          throw (RefConditionFailedException) cause;
        }
        if (cause instanceof RuntimeException) {
          Throwable nestedCause = cause.getCause();
          if (nestedCause instanceof RefNotFoundException) {
            throw (RefNotFoundException) nestedCause;
          }
          if (nestedCause instanceof RefConditionFailedException) {
            throw (RefConditionFailedException) nestedCause;
          }
        }
        cause = cause.getCause();
      }
      throw new RuntimeException(
        "Failed to mark reference as deleted: " + reference.name(),
        e
      );
    }
  }

  @Override
  public void purgeReference(@Nonnull Reference reference)
    throws RefNotFoundException, RefConditionFailedException {
    try {
      database.run(tr -> {
        byte[] key = referenceKey(reference.name());

        // Get current reference
        byte[] current = tr.get(key).join();
        if (current == null) {
          throw new RuntimeException(new RefNotFoundException(reference));
        }

        try {
          Reference currentRef = deserializeReference(current);

          // Verify reference is marked as deleted and matches expected
          if (!currentRef.deleted()) {
            throw new RuntimeException(
              new RefConditionFailedException(reference)
            );
          }
          if (!currentRef.pointer().equals(reference.pointer())) {
            throw new RuntimeException(
              new RefConditionFailedException(reference)
            );
          }

          // Delete the reference
          tr.clear(key);
          return null;
        } catch (Exception e) {
          if (
            e instanceof RefConditionFailedException ||
            e instanceof RefNotFoundException
          ) {
            throw new RuntimeException(e);
          }
          throw new RuntimeException("Failed to purge reference", e);
        }
      });
    } catch (Exception e) {
      if (e instanceof RefNotFoundException) {
        throw (RefNotFoundException) e;
      }
      if (e instanceof RefConditionFailedException) {
        throw (RefConditionFailedException) e;
      }
      // Unwrap nested exceptions
      Throwable cause = e.getCause();
      while (cause != null) {
        if (cause instanceof RefNotFoundException) {
          throw (RefNotFoundException) cause;
        }
        if (cause instanceof RefConditionFailedException) {
          throw (RefConditionFailedException) cause;
        }
        if (cause instanceof RuntimeException) {
          Throwable nestedCause = cause.getCause();
          if (nestedCause instanceof RefNotFoundException) {
            throw (RefNotFoundException) nestedCause;
          }
          if (nestedCause instanceof RefConditionFailedException) {
            throw (RefConditionFailedException) nestedCause;
          }
        }
        cause = cause.getCause();
      }
      throw new RuntimeException(
        "Failed to purge reference: " + reference.name(),
        e
      );
    }
  }

  @Override
  @Nonnull
  public Reference updateReferencePointer(
    @Nonnull Reference reference,
    @Nonnull ObjId newPointer
  ) throws RefNotFoundException, RefConditionFailedException {
    try {
      return database.run(tr -> {
        byte[] key = referenceKey(reference.name());

        // Get current reference
        byte[] current = tr.get(key).join();
        if (current == null) {
          throw new RuntimeException(new RefNotFoundException(reference));
        }

        try {
          Reference currentRef = deserializeReference(current);

          // Verify current reference matches expected
          if (currentRef.deleted()) {
            throw new RuntimeException(
              new RefConditionFailedException(reference)
            );
          }
          if (!currentRef.pointer().equals(reference.pointer())) {
            throw new RuntimeException(
              new RefConditionFailedException(reference)
            );
          }

          // Update with new pointer
          Reference updated = Reference.reference(
            reference.name(),
            newPointer,
            false,
            currentRef.createdAtMicros(),
            currentRef.extendedInfoObj()
          );

          byte[] serialized = serializeReference(updated);
          tr.set(key, serialized);
          return updated;
        } catch (Exception e) {
          if (
            e instanceof RefConditionFailedException ||
            e instanceof RefNotFoundException
          ) {
            throw new RuntimeException(e);
          }
          throw new RuntimeException("Failed to update reference pointer", e);
        }
      });
    } catch (Exception e) {
      if (e instanceof RefNotFoundException) {
        throw (RefNotFoundException) e;
      }
      if (e instanceof RefConditionFailedException) {
        throw (RefConditionFailedException) e;
      }
      // Unwrap nested exceptions
      Throwable cause = e.getCause();
      while (cause != null) {
        if (cause instanceof RefNotFoundException) {
          throw (RefNotFoundException) cause;
        }
        if (cause instanceof RefConditionFailedException) {
          throw (RefConditionFailedException) cause;
        }
        if (cause instanceof RuntimeException) {
          Throwable nestedCause = cause.getCause();
          if (nestedCause instanceof RefNotFoundException) {
            throw (RefNotFoundException) nestedCause;
          }
          if (nestedCause instanceof RefConditionFailedException) {
            throw (RefConditionFailedException) nestedCause;
          }
        }
        cause = cause.getCause();
      }
      throw new RuntimeException(
        "Failed to update reference pointer: " + reference.name(),
        e
      );
    }
  }

  // Object operations

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Obj> T[] fetchTypedObjsIfExist(
    @Nonnull ObjId[] ids,
    ObjType type,
    @Nonnull Class<T> typeClass
  ) {
    return database.run(tr -> {
      T[] results = (T[]) Array.newInstance(typeClass, ids.length);
      List<CompletableFuture<byte[]>> futures = new ArrayList<>();

      for (ObjId id : ids) {
        if (id != null) {
          byte[] key = objectKey(id);
          futures.add(tr.get(key));
        } else {
          futures.add(CompletableFuture.completedFuture(null));
        }
      }

      try {
        for (int i = 0; i < ids.length; i++) {
          if (ids[i] == null) {
            results[i] = null;
            continue;
          }

          byte[] value = futures.get(i).get();
          if (value != null) {
            try {
              Obj obj = deserializeObj(
                ids[i],
                0L,
                ByteBuffer.wrap(value),
                null
              );
              if (type == null || obj.type() == type) {
                results[i] = typeClass.cast(obj);
              }
            } catch (Exception e) {
              throw new RuntimeException(
                "Failed to parse object: " + ids[i],
                e
              );
            }
          }
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException("Failed to fetch objects", e);
      }

      return results;
    });
  }

  @Override
  public boolean storeObj(@Nonnull Obj obj, boolean ignoreSoftSizeRestrictions)
    throws ObjTooLargeException {
    try {
      byte[] serialized = serializeObj(
        obj,
        effectiveIncrementalIndexSizeLimit(),
        effectiveIndexSegmentSizeLimit(),
        false
      );

      if (!ignoreSoftSizeRestrictions) {
        // Check soft size limits
        if (serialized.length > hardObjectSizeLimit()) {
          throw new ObjTooLargeException(
            serialized.length,
            hardObjectSizeLimit()
          );
        }
      }

      return executeWithRetry(tr -> {
        byte[] key = objectKey(obj.id());

        // Check if object already exists
        byte[] existing = tr.get(key).join();
        if (existing != null) {
          return false; // Object already exists
        }

        // Store new object
        tr.set(key, serialized);
        return true;
      });
    } catch (ObjTooLargeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to store object: " + obj.id(), e);
    }
  }

  @Override
  @Nonnull
  public boolean[] storeObjs(@Nonnull Obj[] objs) throws ObjTooLargeException {
    return executeWithRetry(tr -> {
      boolean[] results = new boolean[objs.length];

      for (int i = 0; i < objs.length; i++) {
        if (objs[i] == null) {
          results[i] = false;
          continue;
        }

        try {
          byte[] serialized = serializeObj(
            objs[i],
            effectiveIncrementalIndexSizeLimit(),
            effectiveIndexSegmentSizeLimit(),
            false
          );

          byte[] key = objectKey(objs[i].id());
          byte[] existing = tr.get(key).join();

          if (existing == null) {
            tr.set(key, serialized);
            results[i] = true;
          } else {
            results[i] = false;
          }
        } catch (Exception e) {
          throw new RuntimeException(
            "Failed to serialize object: " + objs[i].id(),
            e
          );
        }
      }

      return results;
    });
  }

  @Override
  public void deleteObj(@Nonnull ObjId id) {
    database.run(tr -> {
      byte[] key = objectKey(id);
      tr.clear(key);
      return null;
    });
  }

  @Override
  public void deleteObjs(@Nonnull ObjId[] ids) {
    database.run(tr -> {
      for (ObjId id : ids) {
        if (id != null) {
          byte[] key = objectKey(id);
          tr.clear(key);
        }
      }
      return null;
    });
  }

  @Override
  public boolean deleteWithReferenced(@Nonnull Obj obj) {
    return executeWithRetry(tr -> {
      byte[] key = objectKey(obj.id());
      byte[] existing = tr.get(key).join();

      if (existing == null) {
        return false;
      }

      try {
        Obj currentObj = deserializeObj(
          obj.id(),
          0L,
          ByteBuffer.wrap(existing),
          null
        );
        if (currentObj.referenced() == obj.referenced()) {
          tr.clear(key);
          return true;
        }
        return false;
      } catch (Exception e) {
        throw new RuntimeException(
          "Failed to parse object for referenced check: " + obj.id(),
          e
        );
      }
    });
  }

  @Override
  public boolean deleteConditional(@Nonnull UpdateableObj obj) {
    return executeWithRetry(tr -> {
      byte[] key = objectKey(obj.id());
      byte[] existing = tr.get(key).join();

      if (existing == null) {
        return false;
      }

      try {
        Obj currentObj = deserializeObj(
          obj.id(),
          0L,
          ByteBuffer.wrap(existing),
          null
        );
        if (currentObj instanceof UpdateableObj) {
          UpdateableObj currentUpdateable = (UpdateableObj) currentObj;
          if (obj.versionToken().equals(currentUpdateable.versionToken())) {
            tr.clear(key);
            return true;
          }
        }
        return false;
      } catch (Exception e) {
        throw new RuntimeException(
          "Failed to parse object for conditional delete: " + obj.id(),
          e
        );
      }
    });
  }

  @Override
  public boolean updateConditional(
    @Nonnull UpdateableObj expected,
    @Nonnull UpdateableObj newValue
  ) throws ObjTooLargeException {
    try {
      byte[] serialized = serializeObj(
        newValue,
        effectiveIncrementalIndexSizeLimit(),
        effectiveIndexSegmentSizeLimit(),
        false
      );

      return executeWithRetry(tr -> {
        byte[] key = objectKey(expected.id());
        byte[] existing = tr.get(key).join();

        if (existing == null) {
          return false;
        }

        try {
          Obj currentObj = deserializeObj(
            expected.id(),
            0L,
            ByteBuffer.wrap(existing),
            null
          );
          if (currentObj instanceof UpdateableObj) {
            UpdateableObj currentUpdateable = (UpdateableObj) currentObj;
            if (
              expected.versionToken().equals(currentUpdateable.versionToken())
            ) {
              tr.set(key, serialized);
              return true;
            }
          }
          return false;
        } catch (Exception e) {
          throw new RuntimeException(
            "Failed to parse object for conditional update: " + expected.id(),
            e
          );
        }
      });
    } catch (Exception e) {
      if (e instanceof ObjTooLargeException) {
        throw e;
      }
      throw new RuntimeException(
        "Failed to serialize object for conditional update: " + newValue.id(),
        e
      );
    }
  }

  @Override
  public void upsertObj(@Nonnull Obj obj) throws ObjTooLargeException {
    try {
      byte[] serialized = serializeObj(
        obj,
        effectiveIncrementalIndexSizeLimit(),
        effectiveIndexSegmentSizeLimit(),
        false
      );

      database.run(tr -> {
        byte[] key = objectKey(obj.id());
        tr.set(key, serialized);
        return null;
      });
    } catch (Exception e) {
      if (e instanceof ObjTooLargeException) {
        throw e;
      }
      throw new RuntimeException("Failed to upsert object: " + obj.id(), e);
    }
  }

  @Override
  public void upsertObjs(@Nonnull Obj[] objs) throws ObjTooLargeException {
    database.run(tr -> {
      for (Obj obj : objs) {
        if (obj != null) {
          try {
            byte[] serialized = serializeObj(
              obj,
              effectiveIncrementalIndexSizeLimit(),
              effectiveIndexSegmentSizeLimit(),
              false
            );

            byte[] key = objectKey(obj.id());
            tr.set(key, serialized);
          } catch (Exception e) {
            throw new RuntimeException(
              "Failed to serialize object for upsert: " + obj.id(),
              e
            );
          }
        }
      }
      return null;
    });
  }

  @Override
  @Nonnull
  public CloseableIterator<Obj> scanAllObjects(
    @Nonnull Set<ObjType> returnedObjTypes
  ) {
    return new FoundationDBObjectIterator(
      database,
      objectKeyPrefix(),
      returnedObjTypes
    );
  }

  @Override
  public void erase() {
    database.run(tr -> {
      // Clear all keys with repository prefix
      byte[] startKey = Tuple.from(keyPrefix).pack();
      byte[] endKey = Tuple.from(keyPrefix + "\uFFFF").pack();
      tr.clear(startKey, endKey);
      return null;
    });
  }

  // Utility methods

  private byte[] referenceKey(String name) {
    return Tuple.from(keyPrefix + "refs", name).pack();
  }

  private byte[] objectKey(ObjId id) {
    return Tuple.from(keyPrefix + "objs", id.toString()).pack();
  }

  private String objectKeyPrefix() {
    return keyPrefix + "objs";
  }

  private <T> T executeWithRetry(Function<Transaction, T> operation) {
    return database.run(operation);
  }

  // Iterator implementation for objects
  private static class FoundationDBObjectIterator
    implements CloseableIterator<Obj> {

    private final Database database;
    private final String keyPrefix;
    private final Set<ObjType> returnedObjTypes;
    private final List<KeyValue> batch;
    private int batchIndex;
    private byte[] continuationKey;
    private boolean finished;

    FoundationDBObjectIterator(
      Database database,
      String keyPrefix,
      Set<ObjType> returnedObjTypes
    ) {
      this.database = database;
      this.keyPrefix = keyPrefix;
      this.returnedObjTypes = returnedObjTypes;
      this.batch = new ArrayList<>();
      this.batchIndex = 0;
      this.continuationKey = null;
      this.finished = false;
    }

    @Override
    public boolean hasNext() {
      while (batchIndex >= batch.size()) {
        if (finished) {
          return false;
        }
        loadNextBatch();
        if (batch.isEmpty()) {
          return false;
        }
      }

      // Look ahead to find the next matching object
      int tempIndex = batchIndex;
      while (tempIndex < batch.size()) {
        KeyValue kv = batch.get(tempIndex);
        try {
          // Extract object ID from key
          Tuple keyTuple = Tuple.fromBytes(kv.getKey());
          String objIdStr = keyTuple.getString(1);
          ObjId objId = ObjId.objIdFromString(objIdStr);

          Obj obj = deserializeObj(
            objId,
            0L,
            ByteBuffer.wrap(kv.getValue()),
            null
          );
          if (
            returnedObjTypes.isEmpty() || returnedObjTypes.contains(obj.type())
          ) {
            return true; // Found a matching object
          }
        } catch (Exception e) {
          // Skip malformed objects
        }
        tempIndex++;
      }

      // No matching objects in current batch, load next batch if not finished
      if (!finished) {
        batchIndex = batch.size(); // Force loadNextBatch on next call
        return hasNext(); // Recursive call to load next batch
      }

      return false;
    }

    @Override
    public Obj next() {
      if (!hasNext()) {
        throw new java.util.NoSuchElementException();
      }

      // hasNext() already verified there's a matching object, so find it
      while (batchIndex < batch.size()) {
        KeyValue kv = batch.get(batchIndex++);
        try {
          // Extract object ID from key
          Tuple keyTuple = Tuple.fromBytes(kv.getKey());
          String objIdStr = keyTuple.getString(1); // Second element is the object ID
          ObjId objId = ObjId.objIdFromString(objIdStr);

          Obj obj = deserializeObj(
            objId,
            0L,
            ByteBuffer.wrap(kv.getValue()),
            null
          );
          if (
            returnedObjTypes.isEmpty() || returnedObjTypes.contains(obj.type())
          ) {
            return obj;
          }
          // If object type doesn't match filter, continue to next iteration
        } catch (Exception e) {
          // Skip malformed objects and continue
        }
      }

      // This should not happen if hasNext() is implemented correctly
      throw new java.util.NoSuchElementException();
    }

    private void loadNextBatch() {
      batch.clear();
      batchIndex = 0;

      database.run(tr -> {
        Range range;

        if (continuationKey != null) {
          // Continue from where we left off
          byte[] endKey = Range.startsWith(Tuple.from(keyPrefix).pack()).end;
          range = new Range(continuationKey, endKey);
        } else {
          // Use FoundationDB's startsWith for proper prefix scanning
          // keyPrefix already includes "objs" from objectKeyPrefix()
          range = Range.startsWith(Tuple.from(keyPrefix).pack());
        }

        List<KeyValue> result = tr.getRange(range, 1000).asList().join(); // Batch size of 1000

        if (result.isEmpty()) {
          finished = true;
        } else {
          batch.addAll(result);
          if (result.size() < 1000) {
            finished = true;
          } else {
            // Set continuation key for next batch
            KeyValue lastKv = result.get(result.size() - 1);
            byte[] lastKey = lastKv.getKey();
            continuationKey = new byte[lastKey.length];
            System.arraycopy(lastKey, 0, continuationKey, 0, lastKey.length);

            // Increment the key slightly to avoid re-reading the same record
            for (int i = continuationKey.length - 1; i >= 0; i--) {
              if (++continuationKey[i] != 0) {
                break;
              }
            }
          }
        }
        return null;
      });
    }

    @Override
    public void close() {
      // Nothing specific to clean up for FoundationDB
    }
  }
}
