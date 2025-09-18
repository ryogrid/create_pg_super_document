# ProcessSyncRequests

## Location
src/backend/storage/sync/sync.c: 286 - 486

## Overview
Processes queued fsync requests during checkpoints, ensuring all modified files are synchronized to disk with comprehensive error handling and performance monitoring.

## Definition
```c
void ProcessSyncRequests(void)
```

## Detailed Description
ProcessSyncRequests is the core function that handles file synchronization during checkpoint operations. It processes all pending fsync requests stored in the pendingOps hash table, ensuring data integrity by forcing all modified files to be written to persistent storage. The function implements sophisticated cycle-based processing to handle new requests that arrive during execution, comprehensive error handling with retry logic for deleted files, and detailed performance monitoring.

The function uses a two-phase approach: first, it absorbs any new sync requests and increments the sync_cycle_ctr to distinguish between old and new entries. Then it processes all entries with the previous cycle counter value, ensuring that new requests arriving during processing are deferred to the next checkpoint. It includes robust error recovery mechanisms, handling cases where files have been deleted or where previous sync attempts failed partway through.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [AbsorbSyncRequests](../A/AbsorbSyncRequests.md) (absorb pending sync requests from other processes)
  - [hash_seq_init](../h/hash_seq_init.md)/hash_seq_search (iterate through hash table entries)
  - syncsw[].sync_syncfiletag (handler-specific file sync function)
  - [hash_search](../h/hash_search.md) (remove processed entries from hash table)
  - INSTR_TIME_* macros (performance timing instrumentation)
  - data_sync_elevel (get appropriate error level for sync failures)
  - FILE_POSSIBLY_DELETED (check if file deletion error is expected)
  - PendingFsyncEntry (structure representing sync requests)
  - FSYNCS_PER_ABSORB (constant for periodic request absorption)
- Called from (representative examples):
  - [CheckPointGuts](../C/CheckPointGuts.md) (main checkpoint processing function in xlog.c:7525)

## Notes and Other Information
- Only called during checkpoints and requires a valid pendingOps table to function
- Implements cycle counter mechanism to handle new requests arriving during processing
- Includes comprehensive retry logic for handling deleted files with proper error classification
- Provides detailed performance metrics including sync timing statistics and longest sync operation
- Handles partial failures gracefully by tracking sync_in_progress state and resetting stale cycle counters
- Periodically calls AbsorbSyncRequests() to prevent request queue overflow during long sync operations
- Respects the enableFsync setting and skips actual syncing when fsync is disabled
- Updates CheckpointStats with performance metrics for monitoring and reporting
- Critical for maintaining ACID properties by ensuring all database changes are durably stored before checkpoint completion