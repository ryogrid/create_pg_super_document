# CancelDBBackends

## Location
[src/backend/storage/ipc/procarray.c:3658-3698](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L3658-L3698)

## Overview
Cancels all backends connected to a specified database by sending them signals, primarily used during recovery conflict resolution to terminate conflicting transactions.

## Definition
```c
void CancelDBBackends(Oid databaseid, ProcSignalReason sigmode, bool conflictPending)
```

## Detailed Description
CancelDBBackends forcefully terminates backends connected to a specific database by sending them process signals. This function is critical for recovery operations, particularly in standby servers where conflicting transactions need to be resolved to allow recovery to proceed.

The function acquires ProcArrayLock in exclusive mode to ensure atomic operation while iterating through all processes. For each matching backend, it:
1. Extracts the virtual transaction ID
2. Sets the recovery conflict pending flag if requested
3. Sends the specified signal to terminate the process

The function can target all databases by passing InvalidOid, or specific databases by their OID. It's designed to handle cases where the target process might have already terminated, ignoring any errors from signal sending.

## Parameters / Member Variables
- `databaseid`: OID of the database whose backends should be canceled. Use InvalidOid to cancel all backends.
- `sigmode`: The type of signal to send (ProcSignalReason enum value)
- `conflictPending`: Whether to mark the process as having a pending recovery conflict

## Dependencies
- Functions called/Symbols referenced:
  - ProcArrayStruct (procArray global variable)
  - LWLockAcquire/LWLockRelease (for ProcArrayLock in LW_EXCLUSIVE mode)
  - PGPROC (process structure)
  - VirtualTransactionId (transaction identifier)
  - GET_VXID_FROM_PGPROC (macro to extract virtual transaction ID)
  - SendProcSignal (function to send signals to processes)
  - ProcSignalReason (enum for signal types)

- Called from (representative examples):
  - ResolveRecoveryConflictWithDatabase (in src/backend/storage/ipc/standby.c:583)
  - SendRecoveryConflictWithBufferPin (in src/backend/storage/ipc/standby.c:887)

## Notes and Other Information
- Uses exclusive lock to ensure no new processes can be added while canceling
- Designed to handle race conditions where processes might terminate between identification and signaling
- Critical for standby server recovery operations when conflicts arise
- The recoveryConflictPending flag helps backends understand why they're being terminated
- Error handling is minimal since the goal is best-effort termination
- Can be used for emergency database shutdown scenarios