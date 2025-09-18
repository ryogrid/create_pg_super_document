# TidStoreLockExclusive

## Location
src/backend/access/common/tidstore.c: 298 - 304

## Overview
Acquires an exclusive lock on a shared TidStore to ensure thread-safe access during write operations to the shared radix tree structure.

## Definition
```c
void TidStoreLockExclusive(TidStore *ts)
```

## Detailed Description
TidStoreLockExclusive provides exclusive locking for shared TidStores to coordinate access between multiple processes. The function checks if the TidStore is shared and, if so, acquires an exclusive lock on the underlying shared radix tree. For local TidStores, no locking is performed since they are only accessed by a single process.

The locking mechanism uses the radix tree's built-in lock support, as the primary data that needs protection is the shared radix tree structure itself. This ensures that write operations (insertions, deletions) are properly serialized across multiple processes accessing the same shared TidStore.

## Parameters / Member Variables
- `ts`: Pointer to the TidStore object to lock (can be either local or shared)

## Dependencies
- Functions called/Symbols referenced:
  - `TidStoreIsShared`
  - `shared_ts_lock_exclusive`
- Called from (representative examples):
  - [do_set_block_offsets](../d/do_set_block_offsets.md) (src/test/modules/test_tidstore/test_tidstore.c:184)

## Notes and Other Information
- Only performs locking for shared TidStores; local TidStores require no locking since they are single-process
- Uses the radix tree's internal locking mechanism rather than external locks
- Must be paired with a corresponding unlock operation to avoid deadlocks
- Primarily used to protect write operations that modify the shared TID data structure
- The lock protects the shared radix tree data, which is the core data structure needing synchronization