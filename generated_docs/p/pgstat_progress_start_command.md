# pgstat_progress_start_command

## Location
[src/backend/utils/activity/backend_progress.c:28-48](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/backend_progress.c#L28-L48)

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
  - [ProgressCommandType](../P/ProgressCommandType.md) (enum type)
  - [PgBackendStatus](../P/PgBackendStatus.md) (struct type)
  - PGSTAT_BEGIN_WRITE_ACTIVITY (macro)
  - MemSet (function)
  - PGSTAT_END_WRITE_ACTIVITY (macro)
- Called from (representative examples):
  - [heap_vacuum_rel](../h/heap_vacuum_rel.md) (VACUUM operations)
  - [analyze_rel](../a/analyze_rel.md) (ANALYZE operations)
  - [DefineIndex](../D/DefineIndex.md) (CREATE INDEX operations)
  - [cluster_rel](../c/cluster_rel.md) (CLUSTER operations)
  - [BeginCopyFrom](../B/BeginCopyFrom.md)/BeginCopyTo (COPY operations)

## Notes and Other Information
- The function checks if progress tracking is enabled (`pgstat_track_activities`) before performing any operations
- Returns immediately if the backend entry (`MyBEEntry`) is not available or progress tracking is disabled
- Uses atomic write operations (PGSTAT_BEGIN/END_WRITE_ACTIVITY) to ensure data consistency
- The progress parameter array is zeroed to provide a clean slate for subsequent progress updates
- This is typically the first function called when starting any trackable long-running operation

## Simplified Source

```c
// Simplified version of pgstat_progress_start_command
void pgstat_progress_start_command(ProgressCommandType cmdtype, Oid relid) {
    volatile PgBackendStatus *beentry = MyBEEntry;

    // Early exit if progress tracking is disabled
    if (!beentry || !pgstat_track_activities)
        return;

    // Set command type and target relation with atomic operations
    PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
    beentry->st_progress_command = cmdtype;
    beentry->st_progress_command_target = relid;
    MemSet(&beentry->st_progress_param, 0, sizeof(beentry->st_progress_param));
    PGSTAT_END_WRITE_ACTIVITY(beentry);
}
```

Key simplifications made:
- Removed verbose comments while preserving essential logic
- Combined condition checks for clarity
- Emphasized the atomic write operation pattern
- Maintained the core functionality of initializing progress tracking