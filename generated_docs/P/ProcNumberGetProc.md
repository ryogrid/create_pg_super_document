# ProcNumberGetProc

## Location
[src/backend/storage/ipc/procarray.c:3137-3158](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L3137-L3158)

## Overview
Returns a backend's PGPROC structure given its process number, providing access to process-specific information for active PostgreSQL backends.

## Definition
PGPROC *ProcNumberGetProc(ProcNumber procNumber)

## Detailed Description
ProcNumberGetProc is a utility function that retrieves the PGPROC structure for a PostgreSQL backend process identified by its process number. The function performs bounds checking to ensure the process number is valid and verifies that the corresponding backend is still active by checking if its PID is non-zero. This function is essential for inter-process communication and monitoring within PostgreSQL, allowing one backend to access information about another backend's state.

The function includes important safety considerations: the returned PGPROC pointer may become invalid arbitrarily quickly if the target backend terminates, so callers must be careful about how they use this information. This is a common pattern in PostgreSQL's concurrent architecture where process states can change rapidly.

## Parameters / Member Variables
- procNumber: The process number (ProcNumber type) of the target backend whose PGPROC structure should be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - GetPGProcByNumber
  - ProcGlobal (global variable access)
- Called from (representative examples):
  - [checkTempNamespaceStatus](../c/checkTempNamespaceStatus.md)
  - [WaitForOlderSnapshots](../W/WaitForOlderSnapshots.md)
  - [LogRecoveryConflict](../L/LogRecoveryConflict.md)
  - [WaitForLockersMultiple](../W/WaitForLockersMultiple.md)
  - [VirtualXactLock](../V/VirtualXactLock.md)

## Notes and Other Information
- Returns NULL if the process number is out of bounds or if the backend is not active (pid == 0)
- The result may become stale immediately after return due to concurrent backend termination
- Callers must handle the possibility of NULL return values
- Part of the process array management infrastructure that enables inter-backend communication
- The function is declared in src/include/storage/procarray.h

## Simplified Source

```c
PGPROC *ProcNumberGetProc(ProcNumber procNumber) {
    PGPROC *result;

    // Check if process number is within valid range
    if (procNumber < 0 || procNumber >= ProcGlobal->allProcCount)
        return NULL;

    // Get the PGPROC structure for this process number
    result = GetPGProcByNumber(procNumber);

    // Return NULL if backend is not active (no PID assigned)
    if (result->pid == 0)
        return NULL;

    return result;
}
```