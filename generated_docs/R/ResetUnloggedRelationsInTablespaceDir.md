# ResetUnloggedRelationsInTablespaceDir

## Location
[src/backend/storage/file/reinit.c:106-160](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/reinit.c#L106-L160)

## Overview
ResetUnloggedRelationsInTablespaceDir processes a single tablespace directory to reset unlogged relations, iterating through database directories within the tablespace and delegating the actual reset work to individual database processing.

## Definition
```c
static void ResetUnloggedRelationsInTablespaceDir(const char *tsdirname, int op)
```

## Detailed Description
This function serves as an intermediate layer in the unlogged relation reset process, focusing on processing a specific tablespace directory. It:

1. Opens the specified tablespace directory (e.g., "base" for default tablespace or "pg_tblspc/[oid]/PG_17_6" for user tablespaces)
2. Iterates through subdirectories, filtering for database directories (identified by numeric names representing database OIDs)
3. Reports progress for each database being processed during startup
4. Delegates the actual reset work to `ResetUnloggedRelationsInDbspaceDir` for each database

The function includes robust error handling, particularly for the case where a tablespace directory doesn't exist (ENOENT), which can occur if a previous DROP TABLESPACE operation was interrupted. Rather than failing startup, it logs the issue and continues.

## Parameters / Member Variables
- `tsdirname`: Path to the tablespace directory to process (e.g., "base", "pg_tblspc/16384/PG_17_6")
- `op`: Bitwise operation flags inherited from parent function
  - `UNLOGGED_RELATION_CLEANUP` (0x0001): Enable cleanup operations
  - `UNLOGGED_RELATION_INIT` (0x0002): Enable initialization operations

## Dependencies
- Functions called/Symbols referenced:
  - `[AllocateDir](../A/AllocateDir.md)`/`ReadDir`/`FreeDir`: Directory traversal operations
  - `ereport`: Error reporting for missing directories
  - `report_startup_progress`: Progress reporting during startup
  - `[ResetUnloggedRelationsInDbspaceDir](ResetUnloggedRelationsInDbspaceDir.md)`: Processes individual database directories
  - `strspn`/`strlen`: String validation for numeric directory names
  - `snprintf`: Path construction

- Called from:
  - `[ResetUnloggedRelations](ResetUnloggedRelations.md)`: For each tablespace directory (lines 75, 90)

## Notes and Other Information
- This is a static function, internal to the reinit.c module
- Uses numeric directory name validation to identify database directories (OID-based)
- Gracefully handles missing tablespace directories from incomplete DROP TABLESPACE operations
- Provides detailed progress reporting showing current database path being processed
- The function distinguishes between cleanup and init operations in progress messages
- Located in src/backend/storage/file/reinit.c:106-160

## Simplified Source

```c
// Simplified version of ResetUnloggedRelationsInTablespaceDir
static void ResetUnloggedRelationsInTablespaceDir(const char *tsdirname, int op) {
    DIR *ts_dir;
    struct dirent *de;
    char dbspace_path[MAXPGPATH * 2];

    // Open tablespace directory
    ts_dir = AllocateDir(tsdirname);

    // Handle missing tablespace gracefully (from incomplete DROP TABLESPACE)
    if (ts_dir == NULL && errno == ENOENT) {
        ereport(LOG, (errmsg("could not open directory \"%s\": %m", tsdirname)));
        return;
    }

    // Process each database directory in the tablespace
    while ((de = ReadDir(ts_dir, tsdirname)) != NULL) {
        // Skip non-numeric directories (only process database OID directories)
        if (strspn(de->d_name, "0123456789") != strlen(de->d_name))
            continue;

        // Build path to database directory
        snprintf(dbspace_path, sizeof(dbspace_path), "%s/%s", tsdirname, de->d_name);

        // Report progress based on operation type
        if (op & UNLOGGED_RELATION_INIT)
            report_startup_progress("resetting unlogged relations (init), current path: %s", dbspace_path);
        else if (op & UNLOGGED_RELATION_CLEANUP)
            report_startup_progress("resetting unlogged relations (cleanup), current path: %s", dbspace_path);

        // Delegate database-specific processing
        ResetUnloggedRelationsInDbspaceDir(dbspace_path, op);
    }

    FreeDir(ts_dir);
}
```

Key simplifications made:
- Removed detailed error handling comments for clarity
- Simplified progress reporting messages by removing elapsed time parameters
- Condensed directory validation logic explanation
- Abstracted complex error reporting into simpler form
- Maintained essential algorithm: open directory → validate entries → process databases → cleanup