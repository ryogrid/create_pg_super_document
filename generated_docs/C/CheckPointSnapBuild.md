# CheckPointSnapBuild

## Location
[src/backend/replication/logical/snapbuild.c:2118-2205](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/snapbuild.c#L2118-L2205)

## Overview
CheckPointSnapBuild performs cleanup of obsolete serialized snapshot files during database checkpoints, removing snapshots that are no longer needed by any replication slot.

## Definition
```c
void CheckPointSnapBuild(void)
```

## Detailed Description
This function is called during database checkpoints to perform maintenance on the logical replication snapshot directory (pg_logical/snapshots). It identifies and removes serialized snapshot files that are no longer required by any active or potential replication slots. The cleanup process:

1. **Cutoff Calculation**: Determines the earliest LSN that might still be needed by:
   - Getting the last redo pointer (earliest point new slots could start)
   - Computing the minimum restart LSN across all existing replication slots
   - Using the more restrictive (earlier) of these two values

2. **Directory Scanning**: Iterates through all files in the pg_logical/snapshots directory

3. **File Parsing**: Extracts LSN values from snapshot filenames (format: X-X.snap)

4. **Age Assessment**: Compares each snapshot's LSN against the cutoff point

5. **Safe Removal**: Deletes snapshots older than the cutoff, with error handling that doesn't block checkpoint completion

The function runs during checkpoints regardless of whether logical decoding is currently enabled, ensuring cleanup of old files even after logical replication is disabled.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [GetRedoRecPtr](../G/GetRedoRecPtr.md)
  - [ReplicationSlotsComputeLogicalRestartLSN](../R/ReplicationSlotsComputeLogicalRestartLSN.md)
  - [AllocateDir](../A/AllocateDir.md)/ReadDir/FreeDir
  - [get_dirent_type](../g/get_dirent_type.md)
  - sscanf
  - unlink
  - ereport/elog
- Called from (representative examples):
  - [CheckPointGuts](CheckPointGuts.md) (in xlog.c:7508)

## Notes and Other Information
- Designed to run during checkpoints as a convenient scheduling point, though not strictly checkpoint-dependent
- Handles both regular snapshot files (.snap) and temporary files (.snap.pid.tmp) from incomplete serializations  
- Uses conservative approach: if unsure, keeps files rather than risking premature deletion
- File parsing failures (malformed filenames) are logged but don't stop the cleanup process
- Unlink failures are logged as warnings but don't prevent checkpoint completion
- The cutoff calculation ensures that no snapshot needed by any current or future replication slot is deleted
- Handles the case where no replication slots exist (cutoff becomes InvalidXLogRecPtr)
- Considers the redo pointer to ensure new replication slots won't need snapshots before that point
- Performs directory-level operations safely with proper resource management (FreeDir cleanup)

## Simplified Source

```c
// Simplified version of CheckPointSnapBuild
void CheckPointSnapBuild(void) {
    XLogRecPtr cutoff;
    XLogRecPtr redo;
    DIR *snap_dir;
    struct dirent *snap_de;
    char path[MAXPGPATH + 21];

    // Step 1: Determine safe cutoff LSN for cleanup
    redo = GetRedoRecPtr();
    cutoff = ReplicationSlotsComputeLogicalRestartLSN();

    // Use the more conservative (earlier) LSN as cutoff
    if (redo < cutoff)
        cutoff = redo;

    // Step 2: Scan snapshot directory for cleanup candidates
    snap_dir = AllocateDir("pg_logical/snapshots");
    while ((snap_de = ReadDir(snap_dir, "pg_logical/snapshots")) != NULL) {
        uint32 hi, lo;
        XLogRecPtr lsn;
        PGFileType de_type;

        // Skip directory entries
        if (skip_directory_entries(snap_de))
            continue;

        snprintf(path, sizeof(path), "pg_logical/snapshots/%s", snap_de->d_name);
        de_type = get_dirent_type(path, snap_de, false, DEBUG1);

        // Only process regular files
        if (de_type != PGFILETYPE_REG)
            continue;

        // Step 3: Parse snapshot filename to extract LSN
        if (sscanf(snap_de->d_name, "%X-%X.snap", &hi, &lo) != 2) {
            // Log parsing failures but continue processing
            report_parsing_error(path);
            continue;
        }

        lsn = ((uint64) hi) << 32 | lo;

        // Step 4: Remove snapshots older than cutoff
        if (lsn < cutoff || cutoff == InvalidXLogRecPtr) {
            elog(DEBUG1, "removing snapbuild snapshot %s", path);

            // Attempt removal with non-fatal error handling
            if (unlink(path) < 0) {
                report_removal_warning(path);
                // Continue processing - don't let cleanup failures stop checkpoints
            }
        }
    }
    FreeDir(snap_dir);
}
```

Key simplifications made:
- Organized into clear steps: cutoff calculation, directory scan, file processing, cleanup
- Abstracted error handling into conceptual helper functions
- Preserved the essential LSN parsing and comparison logic
- Maintained the conservative cleanup approach (keep files when in doubt)
- Simplified directory traversal while preserving safety checks
- Focused on the core algorithm: determine what's safe to remove, then remove it
- Kept the non-blocking error handling that allows checkpoints to complete