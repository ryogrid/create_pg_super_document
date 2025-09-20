# _CloseArchive

## Location
[src/bin/pg_dump/pg_backup_custom.c:740-783](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_custom.c#L740-L783)

## Overview
A mandatory function that finalizes and closes the archive during pg_dump operations, handling the complete archive writing process including header, TOC, and data chunks.

## Definition

```c
static void
_CloseArchive(ArchiveHandle *AH)
```
## Detailed Description
_CloseArchive is responsible for completing the archive creation process in PostgreSQL's custom dump format. When writing an archive, this function orchestrates the final steps of saving the complete dump file to disk. It follows a specific sequence: writing the archive header, writing the Table of Contents (TOC), and writing all data chunks and large objects.

The function includes an optimization where it attempts to rewrite the TOC after data writing is complete, which updates data offset information. This optimization can significantly improve pg_restore performance, especially during parallel restore operations, though pg_restore can function without it.

The function also handles proper file closure and optional file synchronization to ensure data durability.

## Parameters / Member Variables
- : ArchiveHandle pointer containing the archive context, file handle, and configuration settings

## Dependencies
- Functions called/Symbols referenced:
  - [lclContext](../l/lclContext.md) (local context structure type)
  - pgoff_t (PostgreSQL offset type)
  - archModeWrite (archive mode constant)
  - [WriteHead](../W/WriteHead.md) (writes archive header)
  - ftello (gets current file position)
  - [WriteToc](../W/WriteToc.md) (writes table of contents)
  - [WriteDataChunks](../W/WriteDataChunks.md) (writes all data and large objects)
  - fseeko (seeks to specific file position)
  - fclose (closes the file)
  - [fsync_fname](../f/fsync_fname.md) (synchronizes file to disk)
- Called from (representative examples):
  - [InitArchiveFmt_Custom](../I/InitArchiveFmt_Custom.md) (custom format initialization)
  - lclTocEntry (directory format TOC entry handling)
  - [InitArchiveFmt_Directory](../I/InitArchiveFmt_Directory.md) (directory format initialization)  
  - [InitArchiveFmt_Null](../I/InitArchiveFmt_Null.md) (null format initialization)

## Notes and Other Information
- This is a static function specific to the custom archive format implementation
- Located in src/bin/pg_dump/pg_backup_custom.c at lines 740-783
- Only performs write operations when archive mode is archModeWrite
- Includes TOC rewriting optimization for better restore performance
- Handles optional file synchronization based on dosync setting
- Part of the mandatory function interface that archive formats must implement
- The function ensures proper cleanup by setting AH->FH to NULL after closing the file