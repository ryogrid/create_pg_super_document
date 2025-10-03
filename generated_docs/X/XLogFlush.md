# XLogFlush

## Location
[src/backend/access/transam/xlog.c:2779-2966](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L2779-L2966)

## Overview
Ensures that all WAL (Write-Ahead Log) data through a specified position is flushed to disk, implementing group commit optimization and handling both normal operation and recovery scenarios.

## Definition

```c
void
XLogFlush(XLogRecPtr record)
```
## Detailed Description
XLogFlush is a core function in PostgreSQL's WAL system responsible for ensuring data durability by flushing WAL records to disk. The function implements several sophisticated optimization strategies:

1. **Recovery mode handling**: During recovery, it updates the minimum recovery point instead of attempting to flush WAL
2. **Group commit optimization**: Uses CommitDelay and CommitSiblings to batch multiple transactions' flush requests together
3. **Opportunistic batching**: Attempts to flush additional WAL data beyond the requested position to reduce future flush operations
4. **Lock contention management**: Uses LWLockAcquireOrWait to avoid blocking when other processes are already flushing
5. **Critical section protection**: Wraps the main logic in critical sections to ensure atomicity
6. **Corruption resilience**: Handles corrupted LSNs gracefully rather than causing system panic

The function includes special handling for concurrent insertions, waiting for them to complete before proceeding with the flush operation. It also implements a delay mechanism (CommitDelay) that can improve throughput by allowing more transactions to join the group commit.

## Parameters / Member Variables
- `record`: The WAL log sequence number (LSN) that must be flushed to disk before the function returns
## Dependencies
- Functions called/Symbols referenced:
  - [XLogInsertAllowed](XLogInsertAllowed.md)
  - [UpdateMinRecoveryPoint](../U/UpdateMinRecoveryPoint.md)
  - RefreshXLogWriteResult
  - [WaitXLogInsertionsToFinish](../W/WaitXLogInsertionsToFinish.md)
  - [LWLockAcquireOrWait](../L/LWLockAcquireOrWait.md)
  - [MinimumActiveBackends](../M/MinimumActiveBackends.md)
  - [pg_usleep](../p/pg_usleep.md)
  - [XLogWrite](XLogWrite.md)
  - [WalSndWakeupProcessRequests](../W/WalSndWakeupProcessRequests.md)
- Called from (representative examples):
  - [RecordTransactionCommit](../R/RecordTransactionCommit.md)
  - [CreateCheckPoint](../C/CreateCheckPoint.md)
  - [FlushBuffer](../F/FlushBuffer.md)
  - [ReplicationSlotReserveWal](../R/ReplicationSlotReserveWal.md)
  - [SlruPhysicalWritePage](../S/SlruPhysicalWritePage.md)

## Notes and Other Information
- Critical for ACID compliance - ensures committed transactions are durable
- Implements group commit to improve performance under high transaction loads
- Different behavior during recovery vs normal operation
- Uses timeline ID tracking for proper multi-timeline WAL handling
- Includes protection against corrupted LSNs from damaged data pages
- The CommitDelay parameter can significantly impact both latency and throughput
- Wakes up WAL senders after releasing locks to minimize replication lag

## Simplified Source

```c
// Simplified version of XLogFlush
void XLogFlush(XLogRecPtr record) {
    XLogRecPtr WriteRqstPtr;
    XLogwrtRqst WriteRqst;
    TimeLineID insertTLI = XLogCtl->InsertTimeLineID;

    // During recovery, update minimum recovery point instead of flushing
    if (!XLogInsertAllowed()) {
        UpdateMinRecoveryPoint(record, false);
        return;
    }

    // Quick exit if already flushed to the requested position
    if (record <= LogwrtResult.Flush)
        return;

    START_CRIT_SECTION();

    // Initialize write request to target position
    WriteRqstPtr = record;

    // Main flush loop with group commit optimization
    for (;;) {
        XLogRecPtr insertpos;

        // Check if someone else already flushed our data
        RefreshXLogWriteResult(LogwrtResult);
        if (record <= LogwrtResult.Flush)
            break;

        // Wait for pending insertions and get current insert position
        SpinLockAcquire(&XLogCtl->info_lck);
        if (WriteRqstPtr < XLogCtl->LogwrtRqst.Write)
            WriteRqstPtr = XLogCtl->LogwrtRqst.Write;
        SpinLockRelease(&XLogCtl->info_lck);
        insertpos = WaitXLogInsertionsToFinish(WriteRqstPtr);

        // Try to acquire write lock, wait if busy (group commit optimization)
        if (!LWLockAcquireOrWait(WALWriteLock, LW_EXCLUSIVE)) {
            continue; // Lock was busy, retry after someone else may have flushed
        }

        // Recheck if flush is still needed after acquiring lock
        RefreshXLogWriteResult(LogwrtResult);
        if (record <= LogwrtResult.Flush) {
            LWLockRelease(WALWriteLock);
            break;
        }

        // Optional delay for group commit batching
        if (CommitDelay > 0 && enableFsync &&
            MinimumActiveBackends(CommitSiblings)) {
            pg_usleep(CommitDelay);
            // Allow more insertions to complete during delay
            insertpos = WaitXLogInsertionsToFinish(insertpos);
        }

        // Perform the actual write and flush operation
        WriteRqst.Write = insertpos;
        WriteRqst.Flush = insertpos;
        XLogWrite(WriteRqst, insertTLI, false);

        LWLockRelease(WALWriteLock);
        break; // Flush completed successfully
    }

    END_CRIT_SECTION();

    // Wake up WAL senders after releasing locks
    WalSndWakeupProcessRequests(true, !RecoveryInProgress());

    // Verify flush completed successfully
    if (LogwrtResult.Flush < record) {
        elog(ERROR, "xlog flush request %X/%X not satisfied --- flushed only to %X/%X",
             LSN_FORMAT_ARGS(record), LSN_FORMAT_ARGS(LogwrtResult.Flush));
    }
}
```

Key simplifications made:
- Removed detailed comments and debug logging code
- Simplified variable declarations and initialization
- Consolidated error handling into essential checks only
- Removed platform-specific debugging sections
- Streamlined the main loop logic while preserving group commit behavior
- Abstracted complex lock acquisition patterns into clearer flow
- Maintained all critical functionality: recovery handling, group commit optimization, and error detection