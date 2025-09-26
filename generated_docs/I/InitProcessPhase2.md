# InitProcessPhase2

## Location
src/backend/storage/lmgr/proc.c: 493 - 527

## Overview
Makes the current process' PGPROC visible in the shared ProcArray, completing the process initialization sequence.

## Definition
```c
void InitProcessPhase2(void)
```

## Detailed Description
InitProcessPhase2 performs the final step in process initialization by adding the current process to the global ProcArray. This function exists as a separate phase because:

1. **Timing dependency**: Must be called after InitProcess creates the PGPROC structure
2. **EXEC_BACKEND compatibility**: In EXEC_BACKEND builds, ProcArrayAdd requires AttachSharedMemoryStructs to be completed first
3. **LWLock requirement**: ProcArrayAdd needs to acquire LWLocks, which requires a valid PGPROC structure

The function simply adds the current process' PGPROC to the shared ProcArray (making it visible to other processes for transaction management, deadlock detection, etc.) and registers cleanup to remove it during process exit.

Once this function completes, the process is fully integrated into PostgreSQL's multi-process system and visible to other backends for:
- Transaction snapshot creation
- Deadlock detection
- Wait-for-lock analysis
- Process monitoring and statistics

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - ProcArrayAdd (adds process to shared process array)
  - on_shmem_exit (registers exit handler)
  - RemoveProcFromArray (cleanup function for process exit)
- Global variables:
  - MyProc (current process PGPROC, must be non-NULL)
- Called from:
  - InitPostgres (during backend startup sequence)

## Notes and Other Information
- Must be called after InitProcess has successfully created and initialized MyProc
- Separated from InitProcess to handle EXEC_BACKEND timing requirements where shared memory structures need to be attached before ProcArrayAdd can work
- After this call, the process becomes visible to other backends and can participate in global transaction management
- Automatically registers RemoveProcFromArray as an exit handler to ensure proper cleanup
- Essential for proper multi-version concurrency control (MVCC) and transaction isolation
- The process will appear in system views like pg_stat_activity after this initialization