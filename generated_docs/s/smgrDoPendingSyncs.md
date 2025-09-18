# smgrDoPendingSyncs

## Location
src/backend/catalog/storage.c: 725 - 876

## Overview
smgrDoPendingSyncs executes deferred relation file synchronizations at transaction end, either syncing files to disk or emitting WAL records based on file size and truncation status.

## Definition
```c
void smgrDoPendingSyncs(bool isCommit, bool isParallelWorker)
```

## Detailed Description
This function processes pending synchronization operations that were deferred during the transaction to optimize I/O performance. At transaction commit, it decides for each relation whether to physically sync the file to disk or emit WAL records containing the file contents, based on the wal_skip_threshold setting and whether the file was truncated.

For small files (below wal_skip_threshold), the function emits WAL records for all blocks using log_newpage_range(), which can be more efficient as they may be flushed along with other backends' WAL records. For larger files or files that were truncated, it performs actual file synchronization using smgrdosyncall().

The function handles special cases including transaction abort (discards all pending syncs), parallel workers (also discards syncs), and removes relations that are scheduled for deletion to avoid unnecessary work.

## Parameters / Member Variables
- `isCommit`: Boolean indicating whether this is being called at commit (true) or abort (false)
- `isParallelWorker`: Boolean indicating whether this is running in a parallel worker process

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTransactionNestLevel](../G/GetCurrentTransactionNestLevel.md)
  - [AssertPendingSyncs_RelationCache](../A/AssertPendingSyncs_RelationCache.md)
  - [hash_search](../h/hash_search.md)
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - [smgropen](smgropen.md)
  - [smgrexists](smgrexists.md)
  - smgrnblocks
  - [smgrdosyncall](smgrdosyncall.md)
  - [CreateFakeRelcacheEntry](../C/CreateFakeRelcacheEntry.md)
  - [log_newpage_range](../l/log_newpage_range.md)
  - [FreeFakeRelcacheEntry](../F/FreeFakeRelcacheEntry.md)
  - [palloc](../p/palloc.md)
  - [repalloc](../r/repalloc.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [CommitTransaction](../C/CommitTransaction.md)
  - [PrepareTransaction](../P/PrepareTransaction.md)
  - [AbortTransaction](../A/AbortTransaction.md)

## Notes and Other Information
- Only operates at the outermost transaction level (nesting level 1)
- Uses wal_skip_threshold GUC to determine sync vs WAL strategy
- Files that experienced truncation are always synced to prevent trailing garbage blocks after crash recovery
- Removes relations scheduled for deletion from the sync list to avoid unnecessary I/O
- Parallel workers discard pending syncs since sync operations should only be done by the main process
- The is_truncated flag is crucial for determining the sync strategy regardless of file size