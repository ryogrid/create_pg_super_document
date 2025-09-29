# AdvanceNextFullTransactionIdPastXid

## Location
[src/backend/access/transam/varsup.c:304-354](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/varsup.c#L304-L354)

## Overview
AdvanceNextFullTransactionIdPastXid advances the global transaction counter past a specified XID, used during recovery and two-phase commit processing.

## Definition
```c
void AdvanceNextFullTransactionIdPastXid(TransactionId xid)
```

## Detailed Description
AdvanceNextFullTransactionIdPastXid is a specialized function used during database recovery and two-phase commit startup to ensure the global transaction counter is advanced past a specific transaction ID. This function is critical for maintaining transaction ID consistency when replaying WAL records or processing prepared transactions that may have used XIDs from a previous database session. It intelligently handles epoch wraparound by detecting when the target XID would cause a transition to a new epoch, ensuring proper 64-bit FullTransactionId handling even when working with 32-bit XIDs from WAL records.

The function includes safety optimizations: it performs an early return if the target XID is already less than the current next XID, and it can safely read the current state without locks since it only runs in single-process contexts.

## Parameters / Member Variables
- `xid`: The TransactionId that the global counter should advance past

## Dependencies
- Functions called/Symbols referenced:
  - AmStartupProcess
  - XidFromFullTransactionId
  - [TransactionIdFollowsOrEquals](../T/TransactionIdFollowsOrEquals.md)
  - TransactionIdAdvance
  - EpochFromFullTransactionId
  - [FullTransactionIdFromEpochAndXid](../F/FullTransactionIdFromEpochAndXid.md)
- Called from (representative examples):
  - [multixact_redo](../m/multixact_redo.md)
  - [ProcessTwoPhaseBuffer](../P/ProcessTwoPhaseBuffer.md)
  - [xact_redo_commit](../x/xact_redo_commit.md)
  - [xact_redo_abort](../x/xact_redo_abort.md)
  - [ApplyWalRecord](ApplyWalRecord.md)
  - [ProcArrayApplyRecoveryInfo](../P/ProcArrayApplyRecoveryInfo.md)
  - [RecordKnownAssignedTransactionIds](../R/RecordKnownAssignedTransactionIds.md)

## Notes and Other Information
- Located in src/backend/access/transam/varsup.c:304-354
- Only callable during recovery or from two-phase startup code
- Includes assertion to verify it runs only in startup process or single-process mode
- Handles epoch wraparound detection automatically
- Optimized with fast return for XIDs that dont require counter advancement
- Uses exclusive lock only when actually modifying the shared counter
- Essential for maintaining transaction ID consistency across database restarts
- Critical for proper WAL replay and prepared transaction processing

## Simplified Source

```c
// Simplified version of AdvanceNextFullTransactionIdPastXid
void AdvanceNextFullTransactionIdPastXid(TransactionId xid) {
    FullTransactionId newNextFullXid;
    TransactionId next_xid;
    uint32 epoch;

    // Safety check: only startup process can call this
    Assert(AmStartupProcess() || !IsUnderPostmaster);

    // Fast return if XID is not high enough to advance the counter
    next_xid = XidFromFullTransactionId(TransamVariables->nextXid);
    if (!TransactionIdFollowsOrEquals(xid, next_xid))
        return;

    // Compute the next FullTransactionId after the given XID
    // Handle epoch wraparound when XID wraps around
    TransactionIdAdvance(xid);
    epoch = EpochFromFullTransactionId(TransamVariables->nextXid);
    if (unlikely(xid < next_xid))
        ++epoch;
    newNextFullXid = FullTransactionIdFromEpochAndXid(epoch, xid);

    // Update the global transaction counter with proper locking
    LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
    TransamVariables->nextXid = newNextFullXid;
    LWLockRelease(XidGenLock);
}
```

Key simplifications made:
- Preserved all essential logic and error handling
- Simplified complex comments to focus on core functionality
- Maintained original variable names for clarity
- Kept epoch wraparound detection logic intact
- Preserved locking mechanism for thread safety
- Retained fast-path optimization for performance