# AddPendingSync

## Location
src/backend/catalog/storage.c: 85 - 120

## Overview
AddPendingSync queues a relation file for synchronization at transaction commit time, ensuring data durability by deferring fsync operations until the transaction is ready to commit.

## Definition
```c
static void AddPendingSync(const RelFileLocator *rlocator)
```

## Detailed Description
AddPendingSync is a static function that manages deferred synchronization of relation files by maintaining a hash table of pending sync operations. When called, it adds a new entry to the `pendingSyncHash` table, which tracks relations that need to be fsyncd at commit time. The function lazily initializes the hash table on first use, creating it in the TopTransactionContext to ensure proper memory management across transaction boundaries. Each pending sync entry is marked as not truncated by default, allowing the system to optimize sync operations later.

## Parameters / Member Variables
- `rlocator`: Pointer to RelFileLocator structure that uniquely identifies the relation file to be synchronized, containing tablespace, database, and relation OIDs along with fork number

## Dependencies
- Functions called/Symbols referenced:
  - hash_create
  - hash_search
  - PendingRelSync
  - HASHCTL
  - HASH_ELEM
  - HASH_BLOBS
  - HASH_CONTEXT
  - HASH_ENTER
- Called from (representative examples):
  - RelationCreateStorage
  - RestorePendingSyncs

## Notes and Other Information
- The function uses Assert(!found) to ensure that duplicate entries are not added to the hash table
- The hash table is created with 16 initial buckets and uses HASH_BLOBS for key comparison
- Memory allocation occurs in TopTransactionContext to maintain entries across subtransactions
- The is_truncated flag is initialized to false, indicating the relation has not been truncated and requires full synchronization