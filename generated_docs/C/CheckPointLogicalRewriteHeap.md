# CheckPointLogicalRewriteHeap

## Location
[src/backend/access/heap/rewriteheap.c:1155-1253](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/rewriteheap.c#L1155-L1253)

## Overview
Performs checkpoint operations for logical rewrite mapping files by removing obsolete mappings and flushing remaining ones to disk to ensure recovery consistency.

## Definition

```c
struct dirent *mapping_de;
```
## Detailed Description
This function is called during PostgreSQL checkpoints to manage logical rewrite mapping files stored in the pg_logical/mappings directory. It serves two critical purposes: cleanup of obsolete mapping files and ensuring durability of remaining ones.

The function first determines a safe cutoff LSN by considering both the current redo pointer and the logical restart LSNs from existing replication slots. It then scans the mappings directory, parsing filenames to extract metadata (database OID, relation OID, LSN, transaction IDs). Files with LSNs older than the cutoff are safely removed since they're no longer needed for logical decoding. For remaining files, the function performs an fsync to ensure they're durably written to disk.

This checkpoint process ensures that after a crash recovery, logical decoding can reliably continue with only the mapping data that was written after the checkpoint began, maintaining consistency in the logical replication stream.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [GetRedoRecPtr](../G/GetRedoRecPtr.md): Get current redo pointer from shared memory
  - [ReplicationSlotsComputeLogicalRestartLSN](../R/ReplicationSlotsComputeLogicalRestartLSN.md): Calculate minimum restart LSN from replication slots
  - [AllocateDir](../A/AllocateDir.md)/ReadDir/FreeDir: Directory scanning operations
  - [get_dirent_type](../g/get_dirent_type.md): Determine file type from directory entry
  - [OpenTransientFile](../O/OpenTransientFile.md)/CloseTransientFile: File operations with automatic cleanup
  - [pg_fsync](../p/pg_fsync.md): Sync file contents to disk
  - [fsync_fname](../f/fsync_fname.md): Sync directory changes to disk
  - unlink: Remove obsolete mapping files
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md)/pgstat_report_wait_end: Report wait events for monitoring
  - [data_sync_elevel](../d/data_sync_elevel.md): Get appropriate error level for sync operations
- Called from (representative examples):
  - [CheckPointGuts](CheckPointGuts.md): Main checkpoint processing function

## Notes and Other Information
- Critical for maintaining logical decoding consistency across checkpoints and recovery
- Uses LOGICAL_REWRITE_FORMAT to parse mapping file names containing database, relation, LSN, and transaction information
- Safely removes mapping files that are no longer needed based on replication slot restart LSNs
- Ensures durability by explicitly fsyncing remaining mapping files and the directory itself
- Part of the logical decoding infrastructure that supports logical replication and logical backup tools
- Only one checkpoint can run at a time, preventing concurrency issues with file operations
- Files are removed only when they're safely older than any active logical decoding session

## Simplified Source

```c
// Simplified version of CheckPointLogicalRewriteHeap
void CheckPointLogicalRewriteHeap(void) {
    XLogRecPtr cutoff;
    XLogRecPtr redo;
    DIR *mappings_dir;
    struct dirent *mapping_de;
    char path[MAXPGPATH + 20];

    // Step 1: Determine safe LSN cutoff point
    redo = GetRedoRecPtr();
    cutoff = ReplicationSlotsComputeLogicalRestartLSN();

    // Use the more conservative (earlier) LSN as cutoff
    if (cutoff != InvalidXLogRecPtr && redo < cutoff)
        cutoff = redo;

    // Step 2: Scan logical mappings directory
    mappings_dir = AllocateDir("pg_logical/mappings");
    while ((mapping_de = ReadDir(mappings_dir, "pg_logical/mappings")) != NULL) {
        Oid dboid, relid;
        XLogRecPtr lsn;
        TransactionId rewrite_xid, create_xid;
        uint32 hi, lo;

        // Skip directory entries and non-mapping files
        if (skip_non_mapping_files(mapping_de))
            continue;

        // Parse mapping filename to extract metadata
        snprintf(path, sizeof(path), "pg_logical/mappings/%s", mapping_de->d_name);
        if (sscanf(mapping_de->d_name, LOGICAL_REWRITE_FORMAT,
                   &dboid, &relid, &hi, &lo, &rewrite_xid, &create_xid) != 6)
            elog(ERROR, "could not parse filename");

        lsn = ((uint64) hi) << 32 | lo;

        // Step 3: Remove obsolete files or sync current ones
        if (lsn < cutoff || cutoff == InvalidXLogRecPtr) {
            // Remove files older than cutoff LSN
            if (unlink(path) < 0)
                report_file_error("could not remove file", path);
        } else {
            // Sync remaining files to disk for durability
            int fd = OpenTransientFile(path, O_RDWR | PG_BINARY);
            if (fd < 0)
                report_file_error("could not open file", path);

            if (pg_fsync(fd) != 0)
                report_sync_error("could not fsync file", path);

            if (CloseTransientFile(fd) != 0)
                report_file_error("could not close file", path);
        }
    }
    FreeDir(mappings_dir);

    // Step 4: Ensure directory changes are persisted
    fsync_fname("pg_logical/mappings", true);
}
```

Key simplifications made:
- Abstracted file type checking and filename validation into conceptual helper functions
- Simplified error handling while preserving essential error reporting
- Combined related variables in logical groups
- Added clear step-by-step comments for the main algorithm
- Removed detailed error message formatting but kept error handling logic
- Focused on the core checkpoint logic: determine cutoff, scan files, remove/sync, persist directory