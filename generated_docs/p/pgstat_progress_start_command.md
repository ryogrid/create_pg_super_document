# pgstat_progress_start_command

## Location
src/backend/utils/activity/backend_progress.c: 28 - 48

## Overview
Initializes progress tracking for a PostgreSQL backend command by setting the command type and target relation, and zeroing the progress parameter array.

## Definition
```c
void pgstat_progress_start_command(ProgressCommandType cmdtype, Oid relid)
```

## Detailed Description
This function is the entry point for starting progress tracking for various PostgreSQL commands such as VACUUM, ANALYZE, CREATE INDEX, CLUSTER, and COPY operations. It sets up the progress tracking infrastructure in the backend's status entry by:

1. Setting the command type (`st_progress_command`) to identify what operation is being tracked
2. Setting the target relation OID (`st_progress_command_target`) to identify which relation the command operates on
3. Initializing all progress parameters to zero to ensure a clean starting state

The function uses atomic write operations to ensure consistency when updating the backend status information that can be read by other processes.

## Parameters / Member Variables
- `cmdtype`: The type of command being tracked (e.g., PROGRESS_COMMAND_VACUUM, PROGRESS_COMMAND_ANALYZE)
- `relid`: The OID of the relation (table/index) that the command is operating on

## Dependencies
- Functions called/Symbols referenced:
  - ProgressCommandType (enum type)
  - PgBackendStatus (struct type)
  - PGSTAT_BEGIN_WRITE_ACTIVITY (macro)
  - MemSet (function)
  - PGSTAT_END_WRITE_ACTIVITY (macro)
- Called from (representative examples):
  - heap_vacuum_rel (VACUUM operations)
  - analyze_rel (ANALYZE operations)
  - DefineIndex (CREATE INDEX operations)
  - cluster_rel (CLUSTER operations)
  - BeginCopyFrom/BeginCopyTo (COPY operations)

## Notes and Other Information
- The function checks if progress tracking is enabled (`pgstat_track_activities`) before performing any operations
- Returns immediately if the backend entry (`MyBEEntry`) is not available or progress tracking is disabled
- Uses atomic write operations (PGSTAT_BEGIN/END_WRITE_ACTIVITY) to ensure data consistency
- The progress parameter array is zeroed to provide a clean slate for subsequent progress updates
- This is typically the first function called when starting any trackable long-running operation