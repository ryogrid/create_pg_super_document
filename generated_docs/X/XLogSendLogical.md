# XLogSendLogical

## Location
[src/backend/replication/walsender.c:3410-3502](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L3410-L3502)

## Overview
XLogSendLogical streams logically decoded WAL data to replication clients by reading WAL records, processing them through logical decoding, and managing catch-up state for logical replication.

## Definition

```c
static void
XLogSendLogical(void)
```
## Detailed Description
XLogSendLogical is responsible for streaming logically decoded WAL data to logical replication clients. Unlike physical replication which streams raw WAL bytes, logical replication processes WAL records through a decoding context that transforms them into logical change events (INSERT, UPDATE, DELETE operations on tables).

The function operates by:
1. Reading the next available WAL record using the logical decoding context's XLogReader
2. Processing valid records through LogicalDecodingProcessRecord, which invokes the configured output plugin to generate logical change events
3. Determining catch-up status by comparing the current position with the flush/replay pointer
4. Handling graceful shutdown when caught up and stopping is requested

For cascading logical WAL senders (standbys forwarding logical changes), the function uses the replay LSN instead of flush LSN since logical decoding on standbys can only process WAL that has been replayed.

The function maintains a static cache of the flush pointer to avoid repeatedly acquiring contended spinlocks, updating it only when necessary.

## Parameters / Member Variables
This function takes no parameters but operates on several global variables:
- : The logical decoding context containing reader and output plugin state
- : Global flag indicating whether WAL sender has caught up
- : Signal that graceful shutdown has been requested
- : Last WAL position successfully processed
- : Whether this is a cascading WAL sender

## Dependencies
- Functions called/Symbols referenced:
  - [XLogReadRecord](XLogReadRecord.md)
  - [LogicalDecodingProcessRecord](../L/LogicalDecodingProcessRecord.md)
  - [GetXLogReplayRecPtr](../G/GetXLogReplayRecPtr.md)
  - [GetFlushRecPtr](../G/GetFlushRecPtr.md)
  - elog
- Called from (representative examples):
  - [StartLogicalReplication](../S/StartLogicalReplication.md)
  - [WalSndLoop](../W/WalSndLoop.md)

## Notes and Other Information
- Uses a static variable to cache flush pointer across calls for performance optimization
- Logical decoding progress tracking is handled by WalSndUpdateProgress through the output plugin's write API rather than direct LagTrackerWrite calls
- Distinguishes between cascading and non-cascading senders for appropriate LSN reference (replay vs flush)
- Sets got_SIGUSR2 signal when caught up and stopping to ensure orderly connection termination
- Updates shared memory WAL sender status with spinlock protection for concurrent access safety
- Error handling includes detailed error messages for invalid WAL records during logical decoding

## Simplified Source

```c
// Simplified version of XLogSendLogical
static void XLogSendLogical(void) {
    XLogRecord *record;
    char *errm;
    static XLogRecPtr flushPtr = InvalidXLogRecPtr;  // Cached to avoid spinlock contention

    // Assume not caught up until proven otherwise
    WalSndCaughtUp = false;

    // Read next WAL record
    record = XLogReadRecord(logical_decoding_ctx->reader, &errm);

    // Handle read errors
    if (errm != NULL)
        elog(ERROR, "could not find record while sending logically-decoded data: %s", errm);

    // Process valid record through logical decoding
    if (record != NULL) {
        LogicalDecodingProcessRecord(logical_decoding_ctx, logical_decoding_ctx->reader);
        sentPtr = logical_decoding_ctx->reader->EndRecPtr;
    }

    // Update flush pointer when needed
    if (flushPtr == InvalidXLogRecPtr ||
        logical_decoding_ctx->reader->EndRecPtr >= flushPtr) {
        // Use appropriate LSN based on sender type
        if (am_cascading_walsender)
            flushPtr = GetXLogReplayRecPtr(NULL);  // Standby uses replay LSN
        else
            flushPtr = GetFlushRecPtr(NULL);       // Primary uses flush LSN
    }

    // Check if we've caught up
    if (logical_decoding_ctx->reader->EndRecPtr >= flushPtr)
        WalSndCaughtUp = true;

    // Handle graceful shutdown when caught up
    if (WalSndCaughtUp && got_STOPPING)
        got_SIGUSR2 = true;

    // Update shared memory status atomically
    SpinLockAcquire(&MyWalSnd->mutex);
    MyWalSnd->sentPtr = sentPtr;
    SpinLockRelease(&MyWalSnd->mutex);
}
```

Key simplifications made:
- Added clear comments explaining the logical flow
- Simplified flush pointer management logic with descriptive comments
- Explained the cascading vs non-cascading sender logic
- Preserved all essential error handling and state management
- Maintained performance optimization with static flush pointer caching