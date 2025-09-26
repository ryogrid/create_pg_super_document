# BackendPidGetProcWithLock

## Location
[src/backend/storage/ipc/procarray.c:3218-3254](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L3218-L3254)

## Overview
Searches for and returns a backend's PGPROC structure given its PID, assuming the caller already holds the ProcArrayLock for consistent access.

## Definition
PGPROC *BackendPidGetProcWithLock(int pid)

## Detailed Description
BackendPidGetProcWithLock performs the core logic for PID-based process lookup in PostgreSQL's process array. Unlike BackendPidGetProc, this function assumes the caller has already acquired the ProcArrayLock, making it suitable for use in contexts where the lock is held for multiple operations or where the caller needs to ensure the returned PGPROC remains valid for extended use.

The function performs a linear search through the active processes in the procArray, comparing the PID of each process against the target PID. This approach is necessary because PostgreSQL's process array is organized by process numbers rather than PIDs, and PIDs can be reused by the operating system.

The function includes the same safety check as its wrapper to never match dummy PGPROCs (those with PID 0). The linear search is efficient given the typical number of active backend processes, and the lock-holding assumption allows callers to perform additional operations on the found PGPROC without risk of it being deallocated.

## Parameters / Member Variables
- `pid`: The system process ID of the target backend to locate

## Dependencies
- Functions called/Symbols referenced:
  - procArray (global variable access)
  - allProcs (global array access)
  - [ProcArrayStruct](../P/ProcArrayStruct.md)
- Called from (representative examples):
  - [BackendPidGetProc](BackendPidGetProc.md)
  - [GetBlockerStatusData](../G/GetBlockerStatusData.md)

## Notes and Other Information
- Caller must hold ProcArrayLock before calling this function
- Returns NULL if the PID is not found or is 0 (dummy PGPROC)
- Performs linear search through active processes in the process array
- The returned PGPROC remains valid as long as the caller holds the lock
- Used internally by BackendPidGetProc and by functions that need extended access to PGPROC
- More efficient than BackendPidGetProc when multiple operations are needed on the same PGPROC
- The function is declared in src/include/storage/procarray.h