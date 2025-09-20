# ValidateXLOGDirectoryStructure

## Location
[src/backend/access/transam/xlog.c:4076-4137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L4076-L4137)

## Overview
Validates the existence of essential WAL directory structures and recreates missing subdirectories (archive_status and summaries) to support proper WAL management functionality.

## Definition

```c
struct stat stat_buf;
```
## Detailed Description
This function ensures that the PostgreSQL WAL directory structure is properly configured during startup. It performs validation and automatic recovery of missing directory components that are essential for WAL operations.

The function validates three key directories:
1. **pg_wal**: The main WAL directory (must exist, causes FATAL error if missing)
2. **pg_wal/archive_status**: Tracks WAL archiving status (recreated if missing)  
3. **pg_wal/summaries**: Stores WAL summaries (recreated if missing)

The validation process:
1. Checks if pg_wal exists and is a directory (FATAL error if not)
2. For archive_status subdirectory: verifies it exists and is a directory, creates it if missing
3. For summaries subdirectory: verifies it exists and is a directory, creates it if missing

The function is designed to help in Point-In-Time Recovery (PITR) scenarios where someone has copied a PostgreSQL cluster but omitted the pg_wal directory structure. However, it deliberately does not recreate the main pg_wal directory since it's commonly a symlink for performance reasons.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [stat](../s/stat.md)
  - S_ISDIR
  - ereport
  - snprintf
  - MakePGDirectory
  - [errcode_for_file_access](../e/errcode_for_file_access.md)
  - [errmsg](../e/errmsg.md)
- Constants used:
  - XLOGDIR
  - MAXPGPATH
  - FATAL
  - LOG
- Called from:
  - [StartupXLOG](../S/StartupXLOG.md)
  - RefreshXLogWriteResult

## Notes and Other Information
- This function is declared as static, limiting access to within xlog.c
- The function does not recreate the main pg_wal directory by design - it may be a symlink for performance optimization
- Missing subdirectories are recreated automatically with appropriate logging
- The function helps with cluster copying scenarios for PITR purposes
- FATAL errors are generated for missing main directory or when subdirectories exist but aren't directories
- Creation of missing directories is logged at LOG level for administrative visibility
- File path: src/backend/access/transam/xlog.c:4076-4137