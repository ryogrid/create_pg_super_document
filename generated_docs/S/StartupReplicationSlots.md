# StartupReplicationSlots

## Location
[src/backend/replication/slot.c:1892-1952](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L1892-L1952)

## Overview
Loads all replication slots from disk into memory during server startup, performing cleanup of temporary directories and computing required xmin and LSN values.

## Definition

```c
struct dirent *replication_de;
```
## Detailed Description
This function initializes the replication slot system during PostgreSQL server startup by scanning the pg_replslot directory and restoring all valid replication slots from disk into shared memory. The function performs several critical operations:

1. Iterates through all entries in the pg_replslot directory
2. Identifies and cleans up temporary directories (ending with .tmp) that indicate interrupted slot operations
3. Restores valid slot directories by calling RestoreSlotFromDisk
4. Computes the required xmin and LSN values across all restored slots

The function must run before crash recovery begins to ensure that replication slots are properly loaded and their requirements are known to the recovery process.

## Parameters
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  -  (with DEBUG1 level)
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - , 
- Called from:
  -  (src/backend/access/transam/xlog.c:5556)

## Notes and Other Information
- Must be called during server startup before crash recovery begins
- Handles cleanup of interrupted slot operations by removing .tmp directories
- Skips non-directory entries and standard directory entries (. and ..)
- Uses DEBUG1 logging level for directory type determination
- After restoring all slots, computes global xmin and LSN requirements for the replication system
- Early exits if max_replication_slots is 0 or negative
- Performs fsync on the pg_replslot directory after cleaning up temporary directories
- Critical for maintaining replication slot consistency across server restarts

## Simplified Source

```c
// Simplified version of StartupReplicationSlots
void StartupReplicationSlots(void) {
    DIR *replication_dir;
    struct dirent *replication_de;

    elog(DEBUG1, "starting up replication slots");

    // Open the pg_replslot directory
    replication_dir = AllocateDir("pg_replslot");

    // Iterate through all directory entries
    while ((replication_de = ReadDir(replication_dir, "pg_replslot")) != NULL) {
        char path[MAXPGPATH + 12];
        PGFileType de_type;

        // Skip current and parent directory entries
        if (strcmp(replication_de->d_name, ".") == 0 ||
            strcmp(replication_de->d_name, "..") == 0)
            continue;

        // Build full path and check file type
        snprintf(path, sizeof(path), "pg_replslot/%s", replication_de->d_name);
        de_type = get_dirent_type(path, replication_de, false, DEBUG1);

        // Only process directories
        if (de_type != PGFILETYPE_ERROR && de_type != PGFILETYPE_DIR)
            continue;

        // Clean up temporary directories from interrupted operations
        if (pg_str_endswith(replication_de->d_name, ".tmp")) {
            if (!rmtree(path, true)) {
                ereport(WARNING, (errmsg("could not remove directory \"%s\"", path)));
                continue;
            }
            fsync_fname("pg_replslot", true);
            continue;
        }

        // Restore normal slot from disk
        RestoreSlotFromDisk(replication_de->d_name);
    }

    FreeDir(replication_dir);

    // Return early if no slots configured
    if (max_replication_slots <= 0)
        return;

    // Compute global replication requirements
    ReplicationSlotsComputeRequiredXmin(false);
    ReplicationSlotsComputeRequiredLSN();
}
```

Key simplifications made:
- Added clear comments explaining each operation
- Preserved essential directory iteration and cleanup logic
- Maintained temporary directory cleanup mechanism
- Kept all error handling and validation checks
- Simplified variable declarations while preserving functionality
- Maintained proper ordering of operations for replication consistency