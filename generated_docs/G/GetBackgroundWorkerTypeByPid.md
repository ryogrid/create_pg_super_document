# GetBackgroundWorkerTypeByPid

## Location
src/backend/postmaster/bgworker.c: 1296 - 1322

## Overview
Retrieves the background worker type string for a given process ID, providing a way to identify what type of background worker is associated with a specific PID.

## Definition


## Detailed Description
This function searches through the background worker slots to find a worker with the specified process ID and returns its background worker type string. The function uses shared locking on BackgroundWorkerLock to safely access the background worker data structures. It iterates through all allocated background worker slots, comparing the PID of each active slot with the requested PID.

The function returns a pointer to static memory that contains the background worker type string. This design choice means that the returned value must be used before calling the function again, as subsequent calls will overwrite the static buffer. This approach eliminates the need for the caller to manage memory allocation and deallocation, while also avoiding the complexities of the background worker locking protocol for the caller.

## Parameters / Member Variables
- : The process ID of the background worker whose type is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire (with BackgroundWorkerLock, LW_SHARED)
  - LWLockRelease (with BackgroundWorkerLock)
  - strcpy
  - BackgroundWorkerData global structure
  - [BackgroundWorkerSlot](../B/BackgroundWorkerSlot.md) structure
- Called from (representative examples):
  - PG_STAT_GET_ACTIVITY_COLS (in src/backend/utils/adt/pgstatfuncs.c:554)

## Notes and Other Information
- Returns NULL if no background worker with the specified PID is found
- The returned string is stored in static memory (BGW_MAXLEN bytes) and will be overwritten on subsequent calls
- The function acquires a shared lock on BackgroundWorkerLock to ensure thread-safe access to background worker data
- Used primarily for system monitoring and diagnostic purposes, such as in PostgreSQL's system views that display background worker information
- The background worker type string helps identify the specific role or purpose of a background worker process (e.g., "logical replication launcher", "parallel worker", etc.)