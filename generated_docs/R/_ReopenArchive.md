# _ReopenArchive

## Location
src/bin/pg_dump/pg_backup_custom.c: 784 - 828

## Overview
A specialized function that reopens the archive file handle for parallel restore operations, maintaining the current file position while creating a new file handle.

## Definition


## Detailed Description
_ReopenArchive is designed specifically to support parallel restore operations in PostgreSQL's custom dump format. The function creates a new file handle to the same archive file while preserving the current file position, enabling multiple worker processes to read from the same archive simultaneously.

The function includes platform-specific behavior: on Unix-like systems, it closes the original file handle before opening a new one, but on Windows, it keeps the original handle open to avoid issues in multithreading contexts where a thread shouldn't close the parent process's file handle.

The function includes several safety checks to ensure that parallel restore is feasible, including verifying that the archive supports seeking operations and that it's not being read from standard input.

## Parameters / Member Variables
- : ArchiveHandle pointer containing the archive context, file specification, and current file handle

## Dependencies
- Functions called/Symbols referenced:
  - [lclContext](../l/lclContext.md) (local context structure type)
  - pgoff_t (PostgreSQL offset type)
  - archModeWrite (archive mode constant)
  - ftello (gets current file position)
  - fclose (closes file handle - Unix only)
  - PG_BINARY_R (PostgreSQL binary read mode constant)
  - fopen (opens new file handle)
  - fseeko (seeks to specific file position)
- Called from (representative examples):
  - [InitArchiveFmt_Custom](../I/InitArchiveFmt_Custom.md) (custom format initialization)
  - lclTocEntry (directory format TOC entry handling)
  - [InitArchiveFmt_Directory](../I/InitArchiveFmt_Directory.md) (directory format initialization)

## Notes and Other Information
- This is a static function specific to the custom archive format implementation
- Located in src/bin/pg_dump/pg_backup_custom.c at lines 784-828
- Only supports input archives (archModeRead), not output archives
- Requires seekable files and cannot work with standard input
- Platform-specific behavior on Windows vs Unix regarding file handle closure
- Essential for parallel restore functionality
- Preserves the exact file position across the reopen operation
- Part of the archive format interface for supporting parallel operations