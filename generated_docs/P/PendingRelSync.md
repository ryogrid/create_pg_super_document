# PendingRelSync

## Location
[src/backend/catalog/storage.c:70-74](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/storage.c#L70-L74)

## Overview
PendingRelSync is a structure used to track relations that need to be fsyncd at transaction commit, serving as the hash table entry for PostgreSQLs deferred fsync mechanism.

## Definition
```c
typedef struct PendingRelSync
{
    RelFileLocator rlocator;
    bool        is_truncated;    /* Has the file experienced truncation? */
} PendingRelSync;
```

## Detailed Description
PendingRelSync represents a relation that has been modified during a transaction and needs to be synchronized to disk at commit time. This structure is used as entries in the global `pendingSyncHash` hash table to implement PostgreSQLs deferred fsync strategy, which batches file system synchronization operations at transaction end for better performance.

The structure tracks whether the relation file has been truncated during the transaction, which affects how the final sync operation is performed. When a relation is truncated, PostgreSQL needs to handle the sync differently to ensure data consistency.

This deferred sync mechanism is crucial for PostgreSQLs crash safety - it ensures that all relation changes are properly persisted to disk before a transaction is considered committed, while avoiding the performance overhead of immediate syncing after each write operation.

## Parameters / Member Variables
- `rlocator`: RelFileLocator that uniquely identifies the relation file across tablespace, database, and relation number
- `is_truncated`: Boolean flag indicating whether this relation file has experienced truncation during the current transaction

## Dependencies
- Functions called/Symbols referenced:
  - [RelFileLocator](../R/RelFileLocator.md) (struct type for relation identification)

- Called from (representative examples):
  - [AddPendingSync](../A/AddPendingSync.md) (src/backend/catalog/storage.c:87, 96)
  - [RelationPreTruncate](../R/RelationPreTruncate.md) (src/backend/catalog/storage.c:451)
  - [SerializePendingSyncs](../S/SerializePendingSyncs.md) (src/backend/catalog/storage.c:589, 607)
  - [smgrDoPendingSyncs](../s/smgrDoPendingSyncs.md) (src/backend/catalog/storage.c:732, 762)

## Notes and Other Information
- Used as hash table entries in the global `pendingSyncHash` hash table
- The hash table is created with RelFileLocator as the key and PendingRelSync as the entry
- During transaction abort, all pending syncs are simply discarded
- For parallel workers, pending syncs are serialized using SerializePendingSyncs()
- Relations marked for deletion are removed from the sync hash to avoid unnecessary work
- The structure is allocated in TopTransactionContext memory context
- Part of PostgreSQLs Write-Ahead Logging (WAL) and crash recovery system