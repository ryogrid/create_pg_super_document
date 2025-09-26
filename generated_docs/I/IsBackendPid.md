# IsBackendPid

## Location
src/backend/storage/ipc/procarray.c: 3290 - 3322

## Overview
Determines whether a given process ID (PID) corresponds to a currently running PostgreSQL backend process.

## Definition


## Detailed Description
IsBackendPid is a utility function that checks if a specified process ID belongs to an active PostgreSQL backend. This function is designed to be called by external modules rather than by backend processes themselves. It serves as a simple boolean wrapper around BackendPidGetProc, returning true if the PID corresponds to a valid backend process and false otherwise.

The function works by attempting to retrieve the PGPROC structure associated with the given PID through BackendPidGetProc. If a valid PGPROC is found, it indicates that the PID belongs to an active backend; otherwise, the PID is either invalid or corresponds to a process that is not a PostgreSQL backend.

## Parameters / Member Variables
- `pid`: The process ID to check for backend status

## Dependencies
- Functions called/Symbols referenced:
  - BackendPidGetProc
- Called from (representative examples):
  - PG_STAT_GET_SUBSCRIPTION_COLS (in replication/logical/launcher.c)
  - Declared in procarray.h for external module usage

## Notes and Other Information
- This function is specifically intended for use by external modules, not by backend processes
- The function performs a lightweight check by leveraging the existing BackendPidGetProc infrastructure
- Returns false for PID 0 (handled by BackendPidGetProc) as this never matches dummy PGPROCs
- The check is performed under proper locking via BackendPidGetProc to ensure thread safety