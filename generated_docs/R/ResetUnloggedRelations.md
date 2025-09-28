# ResetUnloggedRelations

## Location
[src/backend/storage/file/reinit.c:47-105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/reinit.c#L47-L105)

## Overview
ResetUnloggedRelations is the main entry point function that resets unlogged relations from before the last PostgreSQL restart, processing both cleanup and initialization operations across all tablespaces.

## Definition
```c
void ResetUnloggedRelations(int op)
```

## Detailed Description
This function orchestrates the reset process for unlogged relations after database recovery or restart. It operates in two modes based on the provided operation flags:

1. **UNLOGGED_RELATION_CLEANUP**: Removes all forks of relations that have an "init" fork, except for the "init" fork itself
2. **UNLOGGED_RELATION_INIT**: Copies the "init" fork to the main fork to restore the relation to its initial state

The function systematically processes:
- The default tablespace (pg_default, located in $PGDATA/base)
- All non-default tablespaces (found in pg_tblspc directory)

It uses a temporary memory context to prevent memory leaks during the operation and includes progress reporting functionality for startup operations.

## Parameters / Member Variables
- `op`: Bitwise operation flags controlling the reset behavior
  - `UNLOGGED_RELATION_CLEANUP` (0x0001): Enable cleanup of relation forks
  - `UNLOGGED_RELATION_INIT` (0x0002): Enable initialization from init forks
  - Can be combined with bitwise OR to perform both operations

## Dependencies
- Functions called/Symbols referenced:
  - `AllocSetContextCreate`: Creates temporary memory context
  - `[begin_startup_progress_phase](../b/begin_startup_progress_phase.md)`: Initiates progress reporting
  - `[ResetUnloggedRelationsInTablespaceDir](ResetUnloggedRelationsInTablespaceDir.md)`: Processes individual tablespace directories
  - `[AllocateDir](../A/AllocateDir.md)`/`ReadDir`/`FreeDir`: Directory traversal functions
  - `[MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)`/`MemoryContextDelete`: Memory management
  - `elogog`: Debug logging

- Called from:
  - `[StartupXLOG](../S/StartupXLOG.md)`: During database recovery process (lines 5725, 5874)

## Notes and Other Information
- This function is critical for PostgreSQL's crash recovery mechanism
- Unlogged relations are not WAL-logged, so they must be reset after crashes
- The function processes tablespaces in a specific order: default first, then others
- Uses DEBUG1 logging level to report operation details
- The temporary memory context ensures no memory leaks during directory traversal
- Located in src/backend/storage/file/reinit.c:47-105

## Simplified Source

```c
// Simplified version of ResetUnloggedRelations
void ResetUnloggedRelations(int op) {
    char temp_path[MAXPGPATH + 10 + sizeof(TABLESPACE_VERSION_DIRECTORY)];
    DIR *spc_dir;
    struct dirent *spc_de;
    MemoryContext tmpctx, oldctx;

    // Log the operation being performed
    elog(DEBUG1, "resetting unlogged relations: cleanup %d init %d",
         (op & UNLOGGED_RELATION_CLEANUP) != 0,
         (op & UNLOGGED_RELATION_INIT) != 0);

    // Create temporary memory context to avoid memory leaks
    tmpctx = AllocSetContextCreate(CurrentMemoryContext,
                                   "ResetUnloggedRelations",
                                   ALLOCSET_DEFAULT_SIZES);
    oldctx = MemoryContextSwitchTo(tmpctx);

    // Begin progress reporting for startup operations
    begin_startup_progress_phase();

    // Process unlogged files in default tablespace (pg_default)
    ResetUnloggedRelationsInTablespaceDir("base", op);

    // Process all non-default tablespaces
    spc_dir = AllocateDir("pg_tblspc");

    while ((spc_de = ReadDir(spc_dir, "pg_tblspc")) != NULL) {
        // Skip current and parent directory entries
        if (strcmp(spc_de->d_name, ".") == 0 ||
            strcmp(spc_de->d_name, "..") == 0)
            continue;

        // Build path to tablespace directory
        snprintf(temp_path, sizeof(temp_path), "pg_tblspc/%s/%s",
                 spc_de->d_name, TABLESPACE_VERSION_DIRECTORY);

        // Process this tablespace directory
        ResetUnloggedRelationsInTablespaceDir(temp_path, op);
    }

    FreeDir(spc_dir);

    // Clean up memory context
    MemoryContextSwitchTo(oldctx);
    MemoryContextDelete(tmpctx);
}
```

Key simplifications made:
- Removed detailed comments while preserving essential algorithm documentation
- Maintained the core two-phase processing: default tablespace first, then others
- Kept memory management pattern but simplified context explanations
- Preserved the directory traversal logic with skip conditions
- Maintained proper resource cleanup (directory handle and memory context)
- Kept essential logging and progress reporting calls
- Simplified variable declarations while maintaining functionality