# AuxiliaryPidGetProc

## Location
src/backend/storage/lmgr/proc.c: 1023 - 1070

## Overview
AuxiliaryPidGetProc retrieves the PGPROC structure for an auxiliary process given its process ID (PID), providing access to auxiliary process metadata.

## Definition
```c
PGPROC *AuxiliaryPidGetProc(int pid)
```

## Detailed Description
AuxiliaryPidGetProc searches through the global AuxiliaryProcs array to find the PGPROC entry that matches the given process ID. This function is essential for system monitoring and debugging tasks where you need to access the process control block of auxiliary processes (like background workers, WAL writers, checkpointer, etc.) based on their PID.

The function performs a linear search through all auxiliary process slots, comparing the stored PID in each PGPROC entry with the requested PID. It handles the special case where PID 0 is passed (which represents dummy processes) by immediately returning NULL.

## Parameters / Member Variables
- `pid`: The process ID of the auxiliary process to locate. A value of 0 will always return NULL as it represents dummy PGPROCs.

## Dependencies
- Functions called/Symbols referenced:
  - PGPROC (process control block structure)
  - NUM_AUXILIARY_PROCS (maximum number of auxiliary processes)
  - AuxiliaryProcs (global array of auxiliary process control blocks)

- Called from (representative examples):
  - pg_log_backend_memory_contexts (for memory context debugging)
  - PG_STAT_GET_ACTIVITY_COLS (for process activity statistics)

## Notes and Other Information
- Returns NULL if the PID is not found among auxiliary processes or if PID is 0
- Only searches auxiliary processes, not regular backend processes
- Used primarily for system monitoring and debugging functions
- The search is performed without locking, assuming the auxiliary process array is relatively stable
- Located in src/backend/storage/lmgr/proc.c:1023-1070