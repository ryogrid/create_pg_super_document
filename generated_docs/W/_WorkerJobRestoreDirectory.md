# _WorkerJobRestoreDirectory

## Location
[src/bin/pg_dump/pg_backup_directory.c:849-852](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_directory.c#L849-L852)

## Overview
A worker function executed in child processes during parallel restore operations for directory-format archives that handles the actual data restoration for a single TOC entry.

## Definition
```c
static int _WorkerJobRestoreDirectory(ArchiveHandle *AH, TocEntry *te)
```

## Detailed Description
This function is specifically designed for parallel restore operations in the pg_restore utility when working with directory-format archives. It runs in child processes spawned during parallel restore and is responsible for restoring the actual data content for one Table of Contents (TOC) entry. The function acts as a simple wrapper that delegates all the restoration work to the `parallel_restore` function, maintaining consistency with the parallel processing architecture used throughout the PostgreSQL backup and restore system.

## Parameters / Member Variables
- `AH`: Archive handle containing the restore context and configuration
- `te`: TOC entry representing the database object whose data needs to be restored

## Dependencies
- Functions called/Symbols referenced:
  - [parallel_restore](../p/parallel_restore.md) - Core function that performs the actual data restoration
  - [TocEntry](../T/TocEntry.md) - Type definition for table of contents entries
- Called from (representative examples):
  - Referenced in `lclTocEntry` structure initialization
  - Set up by `InitArchiveFmt_Directory` as part of directory format handler registration

## Notes and Other Information
- Returns the result code from `parallel_restore` directly, allowing proper error propagation
- Part of the parallel restore infrastructure specific to directory-format archives
- Designed for execution in forked child processes during parallel operations
- Complements `_WorkerJobDumpDirectory` by providing the restore counterpart functionality
- The simple wrapper design allows for potential future enhancements or directory-specific processing while maintaining the current interface