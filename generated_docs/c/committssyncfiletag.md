# committssyncfiletag

## Location
[src/backend/access/transam/commit_ts.c:1070-1073](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/commit_ts.c#L1070-L1073)

## Overview
Entry point function for the sync.c subsystem to synchronize commit timestamp files by flushing them to stable storage.

## Definition
```c
int committssyncfiletag(const FileTag *ftag, char *path)
```

## Detailed Description
This function serves as a specialized entry point for PostgreSQL's sync.c subsystem to handle synchronization of commit timestamp files. Commit timestamps are part of PostgreSQL's SLRU (Simple LRU) subsystem that tracks when transactions were committed, which is useful for logical replication and other features that need to know the order of commits.

The function acts as a thin wrapper around the generic SlruSyncFileTag function, providing the commit timestamp-specific control structure (CommitTsCtl) to handle the synchronization. This design pattern allows the sync.c subsystem to work with different types of SLRU files (commit timestamps, transaction status, etc.) through standardized interfaces while each subsystem provides its own entry point with the appropriate control structure.

The synchronization process involves opening the specified file and performing an fsync() operation to ensure data is written to stable storage, which is critical for data durability and recovery.

## Parameters / Member Variables
- `ftag`: Pointer to FileTag structure containing file identification information, particularly the segment number (segno) used to identify which commit timestamp file to sync
- `path`: Character buffer where the full file path will be constructed and returned for the file being synchronized

## Dependencies
- Functions called/Symbols referenced:
  - [SlruSyncFileTag](../S/SlruSyncFileTag.md)
  - CommitTsCtl
  - FileTag (struct type)
- Called from (representative examples):
  - sync.c subsystem (indirectly through function pointer registration)

## Notes and Other Information
- This function is part of PostgreSQL's commit timestamp tracking system, which can be enabled/disabled via the track_commit_timestamp configuration parameter
- The function follows the standard pattern for SLRU sync handlers where each SLRU type provides its own entry point but delegates to the common SlruSyncFileTag implementation
- Return value follows standard Unix convention: 0 on success, -1 on failure with errno set appropriately
- The function is declared in src/include/access/commit_ts.h and implemented in src/backend/access/transam/commit_ts.c
- Commit timestamp files are stored as part of the SLRU subsystem and are critical for logical replication features that need to determine transaction commit ordering