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
  - GetRedoRecPtr: Get current redo pointer from shared memory
  - ReplicationSlotsComputeLogicalRestartLSN: Calculate minimum restart LSN from replication slots
  - AllocateDir/ReadDir/FreeDir: Directory scanning operations
  - get_dirent_type: Determine file type from directory entry
  - OpenTransientFile/CloseTransientFile: File operations with automatic cleanup
  - pg_fsync: Sync file contents to disk
  - fsync_fname: Sync directory changes to disk
  - unlink: Remove obsolete mapping files
  - pgstat_report_wait_start/pgstat_report_wait_end: Report wait events for monitoring
  - data_sync_elevel: Get appropriate error level for sync operations
- Called from (representative examples):
  - CheckPointGuts: Main checkpoint processing function

## Notes and Other Information
- Critical for maintaining logical decoding consistency across checkpoints and recovery
- Uses LOGICAL_REWRITE_FORMAT to parse mapping file names containing database, relation, LSN, and transaction information
- Safely removes mapping files that are no longer needed based on replication slot restart LSNs
- Ensures durability by explicitly fsyncing remaining mapping files and the directory itself
- Part of the logical decoding infrastructure that supports logical replication and logical backup tools
- Only one checkpoint can run at a time, preventing concurrency issues with file operations
- Files are removed only when they're safely older than any active logical decoding session