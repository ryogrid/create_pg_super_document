# _EndLO

## Location
src/bin/pg_dump/pg_backup_custom.c: 391 - 405

## Overview
This function is called by the PostgreSQL archiver when the dumper calls EndLO, marking the completion of a large object dump operation in the custom archive format.

## Definition
```c
static void _EndLO(ArchiveHandle *AH, TocEntry *te, Oid oid)
```

## Detailed Description
_EndLO is a callback function specific to the custom archive format that handles the finalization of large object (LO) dump operations. It performs two key operations: ends the compression stream for the large object data and writes an end marker (value 0) to signal the completion of the large object in the archive stream. This function is part of the custom format's implementation and is registered as a callback during archive format initialization.

## Parameters / Member Variables
- `AH`: Archive handle containing the archive state and format-specific data
- `te`: Table of contents entry for the large object being finalized
- `oid`: Object identifier of the large object being completed

## Dependencies
- Functions called/Symbols referenced:
  - [EndCompressor](EndCompressor.md)
  - [WriteInt](../W/WriteInt.md)
  - [TocEntry](../T/TocEntry.md) (type)
  - [lclContext](../l/lclContext.md) (type)
- Called from (representative examples):
  - [InitArchiveFmt_Custom](../I/InitArchiveFmt_Custom.md) (registered as callback)
  - [InitArchiveFmt_Directory](../I/InitArchiveFmt_Directory.md) (referenced in directory format)
  - [InitArchiveFmt_Null](../I/InitArchiveFmt_Null.md) (referenced in null format)

## Notes and Other Information
This function is marked as optional in the archiver interface and is specific to the custom format implementation. It ensures proper cleanup of compression resources and provides a clear termination marker in the archive stream for large objects.