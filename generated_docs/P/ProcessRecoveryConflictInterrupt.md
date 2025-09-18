# ProcessRecoveryConflictInterrupt

## Location
src/backend/tcop/postgres.c: 3074 - 3231

## Overview
ProcessRecoveryConflictInterrupt handles the resolution of individual recovery conflicts in PostgreSQL's hot standby system by taking appropriate action based on the specific conflict type and current transaction state.

## Definition
```c
static void ProcessRecoveryConflictInterrupt(ProcSignalReason reason)
```

## Detailed Description
ProcessRecoveryConflictInterrupt is a comprehensive conflict resolution function that processes different types of recovery conflicts that can occur during hot standby operations. Recovery conflicts arise when WAL replay on a standby server conflicts with queries currently running on that standby.

The function implements a sophisticated decision tree based on the conflict type and the current state of the backend process. It can take several different actions:

1. **Early return**: If the conflict doesn't apply to the current state (e.g., not waiting for locks when a deadlock conflict occurs)
2. **ERROR**: Abort the current statement/transaction but allow the session to continue
3. **FATAL**: Terminate the entire session when conflicts cannot be resolved safely

Key conflict types handled:
- **STARTUP_DEADLOCK**: Deadlocks between startup process and user queries
- **BUFFERPIN**: Conflicts over buffer pins that delay recovery
- **LOCK**: Lock conflicts with replay operations
- **TABLESPACE**: Tablespace-related conflicts (e.g., temp files)
- **SNAPSHOT**: Snapshot conflicts with replay
- **LOGICALSLOT**: Logical replication slot conflicts
- **DATABASE**: Database drop conflicts

The function considers transaction boundaries, subtransaction state, and protocol synchronization to determine the safest resolution approach.

## Parameters / Member Variables
- `reason`: A ProcSignalReason enum value specifying the type of recovery conflict to process

## Dependencies
- Functions called/Symbols referenced:
  - [IsWaitingForLock](../I/IsWaitingForLock.md) (checks if process is waiting for a lock)
  - HoldingBufferPinThatDelaysRecovery (checks for problematic buffer pins)
  - [GetStartupBufferPinWaitBufId](../G/GetStartupBufferPinWaitBufId.md) (gets startup process buffer wait info)
  - [CheckDeadLockAlert](../C/CheckDeadLockAlert.md) (sets deadlock detection flag)
  - [IsTransactionOrTransactionBlock](../I/IsTransactionOrTransactionBlock.md) (checks transaction state)
  - [IsSubTransaction](../I/IsSubTransaction.md) (checks if in subtransaction)
  - [IsAbortedTransactionBlockState](../I/IsAbortedTransactionBlockState.md) (checks if transaction is already aborted)
  - [LockErrorCleanup](../L/LockErrorCleanup.md) (cleans up lock state before error)
  - [pgstat_report_recovery_conflict](../p/pgstat_report_recovery_conflict.md) (reports conflict statistics)
  - ereport (reports errors with appropriate severity)
  - [errdetail_recovery_conflict](../e/errdetail_recovery_conflict.md) (provides conflict-specific error details)
- Global variables used:
  - MyProc->recoveryConflictPending (marks process as having pending conflict)
  - DoingCommandRead (indicates if waiting for client input)
  - QueryCancelHoldoffCount (prevents interrupts during critical sections)
  - RecoveryConflictPendingReasons (re-arms deferred conflicts)
  - RecoveryConflictPending, InterruptPending (general interrupt flags)
- Called from:
  - [ProcessRecoveryConflictInterrupts](ProcessRecoveryConflictInterrupts.md) (processes all pending conflicts)

## Notes and Other Information
- This is a static function, only called from within the same source file
- The function uses extensive fallthrough logic in its switch statement to handle related conflict types
- LOGICALSLOT conflicts always result in ERROR (never FATAL) due to their specific characteristics
- The function respects QueryCancelHoldoffCount to avoid protocol desynchronization
- Conflicts during command input from client typically result in FATAL to dislodge idle transactions
- Subtransactions generally cannot safely recover from most conflict types, leading to FATAL errors
- The decision between ERROR and FATAL is crucial for maintaining system stability and data consistency
- Each conflict type has specific conditions that determine the appropriate response level