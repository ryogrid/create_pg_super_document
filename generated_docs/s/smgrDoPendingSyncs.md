# smgrDoPendingSyncs

## Location
[src/backend/catalog/storage.c:725-876](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/storage.c#L725-L876)

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
  - [smgrnblocks](smgrnblocks.md)
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

## Simplified Source

```c
// Simplified version of smgrDoPendingSyncs
void smgrDoPendingSyncs(bool isCommit, bool isParallelWorker) {
    PendingRelDelete *pending;
    int nrels = 0, maxrels = 0;
    SMgrRelation *srels = NULL;
    HASH_SEQ_STATUS scan;
    PendingRelSync *pendingsync;

    // Only process at outermost transaction level
    Assert(GetCurrentTransactionNestLevel() == 1);

    // Early exit if no pending syncs
    if (!pendingSyncHash)
        return;

    // Abort case: discard all pending syncs
    if (!isCommit) {
        pendingSyncHash = NULL;
        return;
    }

    // Parallel worker case: discard all pending syncs
    if (isParallelWorker) {
        pendingSyncHash = NULL;
        return;
    }

    // Remove relations scheduled for deletion from sync list
    for (pending = pendingDeletes; pending != NULL; pending = pending->next) {
        if (pending->atCommit) {
            hash_search(pendingSyncHash, &pending->rlocator, HASH_REMOVE, NULL);
        }
    }

    // Process each pending sync operation
    hash_seq_init(&scan, pendingSyncHash);
    while ((pendingsync = (PendingRelSync *) hash_seq_search(&scan))) {
        SMgrRelation srel = smgropen(pendingsync->rlocator, INVALID_PROC_NUMBER);

        // Calculate total blocks across all forks (if not truncated)
        uint64 total_blocks = 0;
        BlockNumber nblocks[MAX_FORKNUM + 1];

        if (!pendingsync->is_truncated) {
            for (ForkNumber fork = 0; fork <= MAX_FORKNUM; fork++) {
                if (smgrexists(srel, fork)) {
                    nblocks[fork] = smgrnblocks(srel, fork);
                    total_blocks += nblocks[fork];
                } else {
                    nblocks[fork] = InvalidBlockNumber;
                }
            }
        }

        // Decide: sync file or emit WAL records
        bool should_sync = (pendingsync->is_truncated ||
                           total_blocks >= wal_skip_threshold * 1024 / BLCKSZ);

        if (should_sync) {
            // Add to list for batch sync
            if (maxrels == 0) {
                maxrels = 8;
                srels = palloc(sizeof(SMgrRelation) * maxrels);
            } else if (maxrels <= nrels) {
                maxrels *= 2;
                srels = repalloc(srels, sizeof(SMgrRelation) * maxrels);
            }
            srels[nrels++] = srel;
        } else {
            // Emit WAL records for small files
            for (ForkNumber fork = 0; fork <= MAX_FORKNUM; fork++) {
                if (BlockNumberIsValid(nblocks[fork])) {
                    Relation rel = CreateFakeRelcacheEntry(srel->smgr_rlocator.locator);
                    log_newpage_range(rel, fork, 0, nblocks[fork], false);
                    FreeFakeRelcacheEntry(rel);
                }
            }
        }
    }

    // Clear pending sync hash
    pendingSyncHash = NULL;

    // Perform batch sync for all collected relations
    if (nrels > 0) {
        smgrdosyncall(srels, nrels);
        pfree(srels);
    }
}
```

Key simplifications made:
- Consolidated variable declarations and removed intermediate calculations
- Simplified the sync vs WAL decision logic into a clear boolean condition
- Abstracted complex memory allocation patterns into readable steps
- Removed detailed comments about implementation specifics, keeping algorithmic flow clear
- Consolidated the fork iteration logic for both block counting and WAL emission
- Emphasized the main decision point: sync large/truncated files, emit WAL for small files
- Preserved all essential error handling and edge cases (abort, parallel worker, deletion conflicts)