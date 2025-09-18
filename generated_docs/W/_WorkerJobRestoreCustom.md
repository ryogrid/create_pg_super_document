# _WorkerJobRestoreCustom

## Location
src/bin/pg_dump/pg_backup_custom.c: 917 - 935

## Overview
This function serves as the worker job entry point for parallel restore operations from custom-format archives in pg_dump/pg_restore.

## Definition
```c
static int _WorkerJobRestoreCustom(ArchiveHandle *AH, TocEntry *te)
```

## Detailed Description
_WorkerJobRestoreCustom is executed in child processes during parallel restore operations from custom-format archives. It acts as a simple wrapper around the generic parallel_restore function, providing the custom archive format's specific implementation for the WorkerJobRestorePtr function pointer in the ArchiveHandle structure. This function is part of the parallel processing framework that allows pg_restore to restore multiple table of contents (TOC) entries concurrently to improve performance.

## Parameters / Member Variables
- `AH`: Archive handle containing format-specific data and function pointers for the custom archive format
- `te`: Table of Contents entry representing the specific database object to be restored by this worker process

## Dependencies
- Functions called/Symbols referenced:
  - [TocEntry](../T/TocEntry.md) (structure type)
  - [parallel_restore](../p/parallel_restore.md) (generic parallel restore function)
- Called from (representative examples):
  - [InitArchiveFmt_Custom](../I/InitArchiveFmt_Custom.md) (through function pointer assignment)

## Notes and Other Information
- This is a static function internal to pg_backup_custom.c
- Executed in child processes during parallel restore operations
- Returns the result code from parallel_restore to indicate success or failure
- Part of the custom archive format's implementation of the parallel restore interface
- The custom format supports parallel restore but not parallel dump (WorkerJobDumpPtr is set to NULL)
- Used in conjunction with _PrepParallelRestore, _Clone, and _DeClone functions for complete parallel restore support