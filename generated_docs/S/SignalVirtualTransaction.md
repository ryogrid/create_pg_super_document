# SignalVirtualTransaction

## Location
[src/backend/storage/ipc/procarray.c:3496-3544](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L3496-L3544)

## Overview
Locates a backend process by its Virtual Transaction ID and sends a specified signal to it, with optional recovery conflict flagging.

## Definition

```c
pid_t
SignalVirtualTransaction(VirtualTransactionId vxid, ProcSignalReason sigmode,
						 bool conflictPending)
```
## Detailed Description
SignalVirtualTransaction is the core function for sending signals to backend processes identified by their Virtual Transaction IDs. It scans the process array to locate the target backend and sends the specified signal using the PostgreSQL inter-process signaling mechanism.

The function operates under a shared ProcArrayLock to safely scan the process array. When it finds a matching virtual transaction ID, it can optionally set the recoveryConflictPending flag on the target process before sending the signal. This flag helps the target backend understand why it's being signaled and how it should respond.

The function is designed to handle race conditions gracefully - if the target process terminates between the lookup and signaling phases, the SendProcSignal call will simply fail harmlessly. This makes the function safe for use in concurrent environments.

## Parameters / Member Variables
- `vxid`: The Virtual Transaction ID of the target backend to signal
- `sigmode`: The type of signal to send (enumerated in ProcSignalReason)
- `conflictPending`: Whether to set the recoveryConflictPending flag on the target process

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease
  - GET_VXID_FROM_PGPROC
  - [SendProcSignal](SendProcSignal.md)
- Called from (representative examples):
  - [CancelVirtualTransaction](../C/CancelVirtualTransaction.md) (in storage/ipc/procarray.c)
  - [ResolveRecoveryConflictWithLock](../R/ResolveRecoveryConflictWithLock.md) (in storage/ipc/standby.c)

## Notes and Other Information
- Returns the PID of the signaled process, or 0 if the target virtual transaction was not found
- Uses shared locking to allow concurrent access while ensuring consistency during the scan
- Handles prepared transactions (pid == 0) by skipping the signal but still setting the conflict flag if needed
- The recoveryConflictPending flag provides context to the target backend about why it's being signaled
- Race conditions between lookup and signaling are handled gracefully through SendProcSignal error handling
- The function performs an exact match on both procNumber and localTransactionId components of the VXID
- Thread-safe operation ensured through proper locking protocols
- Essential building block for hot standby conflict resolution and general backend process management