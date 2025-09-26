# AdvanceNextFullTransactionIdPastXid

## Location
src/backend/access/transam/varsup.c: 304 - 354

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
  - TransactionIdFollowsOrEquals
  - TransactionIdAdvance
  - EpochFromFullTransactionId
  - FullTransactionIdFromEpochAndXid
- Called from (representative examples):
  - multixact_redo
  - ProcessTwoPhaseBuffer
  - xact_redo_commit
  - xact_redo_abort
  - ApplyWalRecord
  - ProcArrayApplyRecoveryInfo
  - RecordKnownAssignedTransactionIds

## Notes and Other Information
- Located in src/backend/access/transam/varsup.c:304-354
- Only callable during recovery or from two-phase startup code
- Includes assertion to verify it runs only in startup process or single-process mode
- Handles epoch wraparound detection automatically
- Optimized with fast return for XIDs that dont require counter advancement
- Uses exclusive lock only when actually modifying the shared counter
- Essential for maintaining transaction ID consistency across database restarts
- Critical for proper WAL replay and prepared transaction processing