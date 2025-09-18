# asyncQueueReadAllNotifications

## Location
src/backend/commands/async.c: 1851 - 2015

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
  - GetLatestSnapshot
  - RegisterSnapshot/UnregisterSnapshot
  - SimpleLruReadPage_ReadOnly
  - SimpleLruGetBankLock
  - asyncQueueProcessPageEntries
  - Various queue position macros (QUEUE_POS_PAGE, QUEUE_POS_OFFSET, etc.)
- Called from (representative examples):
  - Exec_ListenPreCommit
  - ProcessIncomingNotify

## Notes and Other Information
- **Transaction Isolation**: Uses snapshots to ensure only committed notifications are processed
- **Error Safety**: PG_TRY/PG_FINALLY blocks ensure queue position updates even during errors
- **Lock Minimization**: Copies SLRU pages to local buffers to reduce lock hold times
- **Race Condition Handling**: Includes detailed analysis of timing issues between LISTEN and transaction commits
- **Memory Management**: Uses aligned union for page buffer to meet SLRU alignment requirements
- **Static Function**: Internal to async.c, not exposed in public API
- Located in src/backend/commands/async.c:1851-2015