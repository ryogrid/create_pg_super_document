# ProcessSyncRequests

## Location
[src/backend/storage/sync/sync.c:286-486](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/sync/sync.c#L286-L486)

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

## Dependencies
- Functions called/Symbols referenced:
  - [AbsorbSyncRequests](../A/AbsorbSyncRequests.md) (absorb pending sync requests from other processes)
  - [hash_seq_init](../h/hash_seq_init.md)/hash_seq_search (iterate through hash table entries)
  - syncsw[].sync_syncfiletag (handler-specific file sync function)
  - [hash_search](../h/hash_search.md) (remove processed entries from hash table)
  - INSTR_TIME_* macros (performance timing instrumentation)
  - [data_sync_elevel](../d/data_sync_elevel.md) (get appropriate error level for sync failures)
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

## Simplified Source

```c
// Simplified version of ProcessSyncRequests
void ProcessSyncRequests(void) {
    static bool sync_in_progress = false;
    HASH_SEQ_STATUS hstat;
    PendingFsyncEntry *entry;
    int absorb_counter;

    // Performance tracking variables
    int processed = 0;
    instr_time sync_start, sync_end, sync_diff;
    uint64 elapsed, longest = 0, total_elapsed = 0;

    // Validate state
    if (!pendingOps)
        elog(ERROR, "cannot sync without a pendingOps table");

    // Step 1: Absorb any new sync requests from other processes
    AbsorbSyncRequests();

    // Step 2: Handle previous incomplete sync cycles
    if (sync_in_progress) {
        // Previous sync failed - reset stale cycle counters
        reset_stale_cycle_counters();
    }

    // Step 3: Advance cycle counter to distinguish old vs new requests
    sync_cycle_ctr++;
    sync_in_progress = true;

    // Step 4: Process all requests from previous cycle
    absorb_counter = FSYNCS_PER_ABSORB;
    hash_seq_init(&hstat, pendingOps);

    while ((entry = (PendingFsyncEntry *) hash_seq_search(&hstat)) != NULL) {
        // Skip new requests (defer to next checkpoint)
        if (entry->cycle_ctr == sync_cycle_ctr)
            continue;

        Assert((CycleCtr) (entry->cycle_ctr + 1) == sync_cycle_ctr);

        // Step 5: Perform sync operation if fsync is enabled
        if (enableFsync) {
            // Periodically absorb new requests to prevent overflow
            if (--absorb_counter <= 0) {
                AbsorbSyncRequests();
                absorb_counter = FSYNCS_PER_ABSORB;
            }

            // Sync file with retry logic for deleted files
            if (sync_file_with_retry(entry, &sync_start, &sync_end)) {
                // Update performance statistics
                update_sync_statistics(&sync_start, &sync_end, &longest,
                                     &total_elapsed, &processed);
            }
        }

        // Step 6: Remove processed entry from hash table
        remove_processed_entry(entry);
    }

    // Step 7: Record performance metrics and mark completion
    CheckpointStats.ckpt_sync_rels = processed;
    CheckpointStats.ckpt_longest_sync = longest;
    CheckpointStats.ckpt_agg_sync_time = total_elapsed;

    sync_in_progress = false;
}

// Helper function (conceptual)
static bool sync_file_with_retry(PendingFsyncEntry *entry,
                                instr_time *start, instr_time *end) {
    int failures = 0;
    char path[MAXPGPATH];

    while (!entry->canceled) {
        INSTR_TIME_SET_CURRENT(*start);

        if (syncsw[entry->tag.handler].sync_syncfiletag(&entry->tag, path) == 0) {
            INSTR_TIME_SET_CURRENT(*end);
            return true;  // Success
        }

        // Handle retry logic for deleted files
        if (!FILE_POSSIBLY_DELETED(errno) || failures > 0) {
            report_sync_error(path);
            return false;
        }

        // Absorb requests and check for cancellation
        AbsorbSyncRequests();
        failures++;
    }
    return false;  // Entry was canceled
}
```

Key simplifications made:
- Organized into clear sequential steps with descriptive comments
- Abstracted complex retry logic into a conceptual helper function
- Simplified performance tracking while preserving essential metrics
- Maintained the critical cycle counter mechanism for request handling
- Preserved the error handling patterns for deleted files
- Focused on the core algorithm: absorb requests, process old ones, track performance
- Abstracted repetitive operations into conceptual helpers for clarity