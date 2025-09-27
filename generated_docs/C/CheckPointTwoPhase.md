# CheckPointTwoPhase

## Location
[src/backend/access/transam/twophase.c:1807-1888](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L1807-L1888)

## Overview
CheckPointTwoPhase handles the two-phase commit component of PostgreSQL's checkpointing process, ensuring that prepared transaction state files are properly synchronized to disk for durability.

## Definition
```c
void CheckPointTwoPhase(XLogRecPtr redo_horizon)
```

## Detailed Description
CheckPointTwoPhase is responsible for persisting the state of two-phase commit transactions (prepared transactions) during checkpoint operations. The function operates under the principle that most prepared transactions have short lifespans, so it runs late in the checkpoint sequence to minimize the number of transactions that need disk serialization.

The function iterates through all prepared transactions and identifies those that:
- Are valid or have been generated during recovery (valid || inredo)
- Are not already on disk (!ondisk)  
- Have a PREPARE LSN that precedes or equals the checkpoint's redo horizon

For qualifying transactions, it reads the transaction data from WAL, recreates the two-phase state file on disk, and marks the transaction as persisted. The function holds TwoPhaseStateLock throughout the operation to prevent new transactions from preparing during the checkpoint.

## Parameters / Member Variables
- `redo_horizon`: XLogRecPtr specifying the WAL position up to which prepared transactions must be checkpointed

## Dependencies
- Functions called/Symbols referenced:
  - [XlogReadTwoPhaseData](../X/XlogReadTwoPhaseData.md)
  - [RecreateTwoPhaseFile](../R/RecreateTwoPhaseFile.md)
  - [fsync_fname](../f/fsync_fname.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease
  - [errmsg_plural](../e/errmsg_plural.md)
- Called from:
  - [CheckPointGuts](CheckPointGuts.md)

## Notes and Other Information
- Deliberately runs late in the checkpoint sequence to allow short-lived prepared transactions to complete
- Holds TwoPhaseStateLock in shared mode during I/O operations for simplicity
- Unconditionally fsyncs the TWOPHASE_DIR to ensure directory changes are durable
- Logs the number of serialized transactions when log_checkpoints is enabled
- Returns early if max_prepared_xacts <= 0 (two-phase commit disabled)
- Uses DTrace tracing points for performance monitoring

## Simplified Source

```c
// Simplified version of CheckPointTwoPhase
void CheckPointTwoPhase(XLogRecPtr redo_horizon) {
    int i;
    int serialized_xacts = 0;

    // Early exit if two-phase commit is disabled
    if (max_prepared_xacts <= 0)
        return;

    TRACE_POSTGRESQL_TWOPHASE_CHECKPOINT_START();

    // Step 1: Process all prepared transactions under lock
    LWLockAcquire(TwoPhaseStateLock, LW_SHARED);

    for (i = 0; i < TwoPhaseState->numPrepXacts; i++) {
        GlobalTransaction gxact = TwoPhaseState->prepXacts[i];

        // Step 2: Identify transactions that need serialization
        if (should_serialize_gxact(gxact, redo_horizon)) {
            char *buf;
            int len;

            // Step 3: Read transaction data from WAL and write to disk
            XlogReadTwoPhaseData(gxact->prepare_start_lsn, &buf, &len);
            RecreateTwoPhaseFile(gxact->xid, buf, len);

            // Step 4: Mark transaction as persisted and cleanup
            mark_gxact_on_disk(gxact);
            pfree(buf);
            serialized_xacts++;
        }
    }

    LWLockRelease(TwoPhaseStateLock);

    // Step 5: Ensure directory changes are durable
    fsync_fname(TWOPHASE_DIR, true);

    TRACE_POSTGRESQL_TWOPHASE_CHECKPOINT_DONE();

    // Step 6: Log checkpoint activity if enabled
    if (log_checkpoints && serialized_xacts > 0)
        log_serialized_transactions(serialized_xacts);
}

// Helper function (conceptual)
static bool should_serialize_gxact(GlobalTransaction gxact, XLogRecPtr redo_horizon) {
    return (gxact->valid || gxact->inredo) &&
           !gxact->ondisk &&
           gxact->prepare_end_lsn <= redo_horizon;
}

// Helper function (conceptual)
static void mark_gxact_on_disk(GlobalTransaction gxact) {
    gxact->ondisk = true;
    gxact->prepare_start_lsn = InvalidXLogRecPtr;
    gxact->prepare_end_lsn = InvalidXLogRecPtr;
}
```

Key simplifications made:
- Organized into clear sequential steps with descriptive comments
- Extracted complex conditions into a conceptual helper function for readability
- Abstracted transaction state updates into a helper function
- Simplified error handling and logging while preserving essential functionality
- Maintained the critical WAL reading and file recreation logic
- Preserved the locking strategy and directory synchronization
- Focused on the core algorithm: find qualifying transactions, serialize them, ensure durability