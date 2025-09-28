# asyncQueueReadAllNotifications

## Location
[src/backend/commands/async.c:1851-2015](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L1851-L2015)

## Overview
Reads all pending notifications from the notification queue and delivers appropriate ones to the frontend, processing entries from the current backend's position up to the queue head or first uncommitted notification.

## Definition
```c
static void asyncQueueReadAllNotifications(void)
```

## Detailed Description
This function is the core mechanism for reading and processing NOTIFY messages from PostgreSQL's shared notification queue. It implements a sophisticated algorithm that handles concurrent access, transaction isolation, and error recovery.

The function operates in several key phases:

1. **State Acquisition**: Acquires the current queue head position and the backend's current position in the queue while holding NotifyQueueLock.

2. **Snapshot Management**: Takes a transaction snapshot to determine which transactions are still in progress. This is critical for ensuring that only committed notifications are processed, maintaining ACID properties.

3. **Page-by-Page Processing**: Reads queue pages using the SLRU (Simple Least Recently Used) buffer manager, copying data to local buffers to minimize lock contention.

4. **Safe Error Handling**: Uses PostgreSQL's PG_TRY/PG_FINALLY exception handling to ensure that the backend's queue position is always updated, even if errors occur during message transmission to the frontend.

The function includes extensive commentary about race conditions and transaction isolation issues, particularly regarding the timing of LISTEN commands relative to transaction commits.

## Parameters / Member Variables
This function takes no parameters and operates on global state variables and the shared notification queue.

## Dependencies
- Functions called/Symbols referenced:
  - [GetLatestSnapshot](../G/GetLatestSnapshot.md)
  - [RegisterSnapshot](../R/RegisterSnapshot.md)/UnregisterSnapshot
  - [SimpleLruReadPage_ReadOnly](../S/SimpleLruReadPage_ReadOnly.md)
  - [SimpleLruGetBankLock](../S/SimpleLruGetBankLock.md)
  - [asyncQueueProcessPageEntries](asyncQueueProcessPageEntries.md)
  - Various queue position macros (QUEUE_POS_PAGE, QUEUE_POS_OFFSET, etc.)
- Called from (representative examples):
  - [Exec_ListenPreCommit](../E/Exec_ListenPreCommit.md)
  - [ProcessIncomingNotify](../P/ProcessIncomingNotify.md)

## Notes and Other Information
- **Transaction Isolation**: Uses snapshots to ensure only committed notifications are processed
- **Error Safety**: PG_TRY/PG_FINALLY blocks ensure queue position updates even during errors
- **Lock Minimization**: Copies SLRU pages to local buffers to reduce lock hold times
- **Race Condition Handling**: Includes detailed analysis of timing issues between LISTEN and transaction commits
- **Memory Management**: Uses aligned union for page buffer to meet SLRU alignment requirements
- **Static Function**: Internal to async.c, not exposed in public API
- Located in src/backend/commands/async.c:1851-2015

## Simplified Source

```c
// Simplified version of asyncQueueReadAllNotifications
static void asyncQueueReadAllNotifications(void) {
    QueuePosition current_pos;
    QueuePosition queue_head;
    Snapshot snapshot;
    char page_buffer[QUEUE_PAGESIZE];

    // Step 1: Get current state from shared queue
    LWLockAcquire(NotifyQueueLock, LW_SHARED);
    current_pos = QUEUE_BACKEND_POS(MyProcNumber);
    queue_head = QUEUE_HEAD;
    LWLockRelease(NotifyQueueLock);

    // Early exit if no new notifications
    if (QUEUE_POS_EQUAL(current_pos, queue_head)) {
        return; // Already read everything
    }

    // Step 2: Take snapshot to determine which transactions are committed
    // This ensures we only process notifications from committed transactions
    snapshot = RegisterSnapshot(GetLatestSnapshot());

    // Step 3: Process notifications with error-safe position tracking
    PG_TRY(); {
        bool reached_end = false;

        // Read and process pages until we reach the head or uncommitted entry
        while (!reached_end) {
            int64 page_num = QUEUE_POS_PAGE(current_pos);
            int page_offset = QUEUE_POS_OFFSET(current_pos);

            // Read page from SLRU into local buffer
            int slot = SimpleLruReadPage_ReadOnly(NotifyCtl, page_num, InvalidTransactionId);

            // Determine how much of the page to copy
            int copy_size;
            if (page_num == QUEUE_POS_PAGE(queue_head)) {
                copy_size = QUEUE_POS_OFFSET(queue_head) - page_offset;
            } else {
                copy_size = QUEUE_PAGESIZE - page_offset;
            }

            // Copy page data to local buffer and release SLRU lock
            memcpy(page_buffer + page_offset,
                   NotifyCtl->shared->page_buffer[slot] + page_offset,
                   copy_size);
            LWLockRelease(SimpleLruGetBankLock(NotifyCtl, page_num));

            // Process all entries in this page section
            reached_end = asyncQueueProcessPageEntries(&current_pos, queue_head,
                                                       page_buffer, snapshot);
        }
    }
    PG_FINALLY(); {
        // Always update our position in shared state, even if errors occurred
        LWLockAcquire(NotifyQueueLock, LW_SHARED);
        QUEUE_BACKEND_POS(MyProcNumber) = current_pos;
        LWLockRelease(NotifyQueueLock);
    }
    PG_END_TRY();

    // Cleanup snapshot
    UnregisterSnapshot(snapshot);
}
```

Key simplifications made:
- Removed extensive comments about race conditions and transaction timing
- Simplified variable declarations and eliminated the alignment union
- Consolidated error handling logic while preserving the PG_TRY/PG_FINALLY pattern
- Reduced complex queue position calculations to essential logic
- Abstracted low-level memory operations with clearer variable names
- Preserved the core algorithm: fetch state → take snapshot → process pages → update position