# SendProcSignal

## Location
[src/backend/storage/ipc/procsignal.c:257-328](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procsignal.c#L257-L328)

## Overview
Sends a signal to a PostgreSQL process by setting a signal flag in shared memory and delivering a SIGUSR1 signal, with optional process number optimization for faster lookups.

## Definition

```c
int
SendProcSignal(pid_t pid, ProcSignalReason reason, ProcNumber procNumber)
```
## Detailed Description
SendProcSignal is the primary function for sending inter-process signals within PostgreSQL. It works by first setting a signal flag in the target process's shared memory slot, then sending a SIGUSR1 signal to wake up the target process. The function supports two modes of operation: if a valid procNumber is provided, it directly accesses the corresponding slot; otherwise, it searches through all slots to find the one matching the given PID. The search is performed backward to optimize for auxiliary processes that typically occupy slots near the end of the array. The function includes race condition handling and returns appropriate error codes.

## Parameters / Member Variables
- `pid`: Process ID of the target process to signal
- `reason`: The reason/type of signal being sent (from ProcSignalReason enum)
- `procNumber`: Optional process number for direct slot access (use INVALID_PROC_NUMBER to trigger PID search)
## Dependencies
- Functions called/Symbols referenced:
  - kill
  - pid_t (type)
  - ProcNumber (type)
  - ProcSignalReason (type)
  - [ProcSignalSlot](../P/ProcSignalSlot.md) (type)
  - INVALID_PROC_NUMBER (constant)
  - SIGUSR1 (constant)
  - NumProcSignalSlots (variable)
- Called from (representative examples):
  - [SignalBackends](SignalBackends.md)
  - [SignalVirtualTransaction](SignalVirtualTransaction.md)
  - [CancelDBBackends](../C/CancelDBBackends.md)
  - [WalSndInitStopping](../W/WalSndInitStopping.md)
  - [ParallelWorkerShutdown](../P/ParallelWorkerShutdown.md)

## Notes and Other Information
- Returns 0 on success, -1 on error with errno set (typically ESRCH or EPERM)
- Handles race conditions gracefully - signals are designed to be safe if sent to wrong process
- Atomically sets signal flags before sending the actual signal
- Searches array backward when procNumber not provided to optimize for auxiliary processes
- Not to be confused with ProcSendSignal (different function)
- Signal flags are checked by target process in signal handler or polling loop
- Located in src/backend/storage/ipc/procsignal.c:257-328

## Simplified Source

```c
// Simplified version of SendProcSignal
int SendProcSignal(pid_t pid, ProcSignalReason reason, ProcNumber procNumber) {
    volatile ProcSignalSlot *slot;

    // Fast path: Use provided process number for direct slot access
    if (procNumber != INVALID_PROC_NUMBER) {
        slot = &ProcSignal->psh_slot[procNumber];

        // Verify PID matches the slot
        if (slot->pss_pid == pid) {
            // Set signal flag and send SIGUSR1
            slot->pss_signalFlags[reason] = true;
            return kill(pid, SIGUSR1);
        }
    }
    else {
        // Slow path: Search all slots by PID (backward for auxiliary processes)
        for (int i = NumProcSignalSlots - 1; i >= 0; i--) {
            slot = &ProcSignal->psh_slot[i];

            if (slot->pss_pid == pid) {
                // Set signal flag and send SIGUSR1
                slot->pss_signalFlags[reason] = true;
                return kill(pid, SIGUSR1);
            }
        }
    }

    // Process not found
    errno = ESRCH;
    return -1;
}
```

Key simplifications made:
- Removed detailed race condition comments for clarity
- Consolidated duplicate signal flag setting and kill() calls
- Simplified variable declarations
- Focused on the two main execution paths: direct access vs. search
- Preserved essential error handling and return values
- Maintained the backward search optimization for auxiliary processes