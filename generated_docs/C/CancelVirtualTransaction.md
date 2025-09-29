# CancelVirtualTransaction

## Location
[src/backend/storage/ipc/procarray.c:3490-3495](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L3490-L3495)

## Overview
Cancels a specific virtual transaction by sending a signal to the associated backend process, primarily used in recovery conflict processing scenarios.

## Definition

```c
pid_t
CancelVirtualTransaction(VirtualTransactionId vxid, ProcSignalReason sigmode)
```
## Detailed Description
CancelVirtualTransaction is a wrapper function around SignalVirtualTransaction that simplifies the cancellation of virtual transactions during recovery conflict resolution. It specifically sets the conflictPending flag to true when signaling the target transaction, indicating that this is a cancellation due to a recovery conflict.

This function is typically used when the recovery process on a standby server needs to cancel active transactions that conflict with recovery operations, such as when applying WAL records that would conflict with currently running queries.

The function searches through the process array to find the backend with the matching virtual transaction ID and sends the specified signal to that process. If the process is found and successfully signaled, it returns the process ID; otherwise, it returns 0.

## Parameters / Member Variables
- `vxid`: The Virtual Transaction ID of the transaction to cancel
- `sigmode`: The type of signal to send to the backend process (ProcSignalReason enum value)

## Dependencies
- Functions called/Symbols referenced:
  - [SignalVirtualTransaction](../S/SignalVirtualTransaction.md)
- Called from (representative examples):
  - [ResolveRecoveryConflictWithVirtualXIDs](../R/ResolveRecoveryConflictWithVirtualXIDs.md) (in storage/ipc/standby.c)

## Notes and Other Information
- This is a thin wrapper around SignalVirtualTransaction with conflictPending set to true
- Returns the PID of the signaled process, or 0 if the target transaction was not found
- Primarily used in hot standby conflict resolution scenarios
- The conflictPending flag being set to true helps the target backend understand that it's being canceled due to a recovery conflict
- The function handles the case where the target process might have already terminated between lookup and signaling
- Thread-safe operation through proper use of ProcArrayLock in the underlying SignalVirtualTransaction function

## Simplified Source
```c
pid_t CancelVirtualTransaction(VirtualTransactionId vxid, ProcSignalReason sigmode) {
    // Delegate to SignalVirtualTransaction with conflict pending flag set to true
    // This indicates the cancellation is due to a recovery conflict
    return SignalVirtualTransaction(vxid, sigmode, true);
}
```