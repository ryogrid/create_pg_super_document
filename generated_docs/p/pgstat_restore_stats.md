# pgstat_restore_stats

## Location
[src/backend/utils/activity/pgstat.c:407-418](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat.c#L407-L418)

## Overview
Restores PostgreSQL statistics from the on-disk statistics file into shared memory at server startup. This function serves as the entry point for loading previously persisted statistics data.

## Definition

```c
void
pgstat_restore_stats(void)
```
## Detailed Description
This function is responsible for reading and restoring PostgreSQL statistics from the persistent statistics file into memory when the server starts up. It acts as a simple wrapper around the more complex  function that performs the actual file reading and data restoration.

The function should only be called during server startup by either the startup process or when running in single-user mode. This ensures that statistics data persisted from the previous server session is available to the current session, providing continuity of statistical information across server restarts.

The restoration process includes reading various types of statistics such as archiver stats, bgwriter stats, checkpointer stats, IO stats, SLRU stats, WAL stats, and hash table entries for database objects.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_read_statsfile](pgstat_read_statsfile.md)
- Called from (representative examples):
  - [StartupXLOG](../S/StartupXLOG.md) (src/backend/access/transam/xlog.c:5640)

## Notes and Other Information
- This function is part of the PostgreSQL statistics collection system
- Should only be executed by the startup process or in single-user mode to avoid concurrency issues
- The actual statistics file reading and parsing is delegated to 
- If the statistics file doesn't exist or is corrupted, the system will start with empty statistics counters
- The function is located in src/backend/utils/activity/pgstat.c:407-418

## Simplified Source

```c
// Simplified version of pgstat_restore_stats
void pgstat_restore_stats(void) {
    // Read statistics from persistent file into memory
    pgstat_read_statsfile();
}
```

Key simplifications made:
- No simplifications needed - function is already minimal
- Acts as a simple wrapper around pgstat_read_statsfile()
- Single responsibility: restore stats from disk at startup