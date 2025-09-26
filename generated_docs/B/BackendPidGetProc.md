# BackendPidGetProc

## Location
src/backend/storage/ipc/procarray.c: 3195 - 3217

## Overview
Retrieves a backend's PGPROC structure given its process ID (PID), providing a thread-safe way to look up process information by PID.

## Definition
PGPROC *BackendPidGetProc(int pid)

## Detailed Description
BackendPidGetProc is a thread-safe wrapper function that finds and returns the PGPROC structure for a PostgreSQL backend process identified by its system process ID. The function acquires the ProcArrayLock in shared mode to ensure consistent access to the process array while searching for the specified PID.

This function serves as the primary interface for PID-based process lookups in PostgreSQL, delegating the actual search logic to BackendPidGetProcWithLock while handling the locking protocol. It's commonly used by monitoring functions, signal handling routines, and administrative operations that need to locate a specific backend process.

The function includes a special check to never match dummy PGPROCs (those with PID 0), which are used as placeholders in the process array. Like other process introspection functions, callers must be aware that the returned information may become stale quickly due to process termination or other concurrent changes.

## Parameters / Member Variables
- `pid`: The system process ID of the target backend to locate

## Dependencies
- Functions called/Symbols referenced:
  - BackendPidGetProcWithLock
  - LWLockAcquire
  - LWLockRelease
  - ProcArrayLock (global lock)
- Called from (representative examples):
  - IsBackendPid
  - TerminateOtherDBBackends
  - pg_signal_backend
  - pg_log_backend_memory_contexts
  - pg_stat_get_backend_wait_event_type
  - pg_isolation_test_session_is_blocked

## Notes and Other Information
- Returns NULL if the PID is not found or is 0 (dummy PGPROC)
- Uses ProcArrayLock in shared mode for thread-safe access
- Wrapper function that delegates to BackendPidGetProcWithLock for the actual search
- Widely used by system administration and monitoring functions
- The returned PGPROC pointer may become invalid if the target process terminates
- Callers are responsible for ensuring the meaningfulness of the query over time
- The function is declared in src/include/storage/procarray.h