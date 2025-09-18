# _PrepParallelRestore

## Location
[src/bin/pg_dump/pg_backup_custom.c:829-880](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_custom.c#L829-L880)

## Overview
A specialized function that prepares archive data for parallel restore by calculating data length information for TOC entries to optimize job ordering during parallel operations.

## Definition


## Detailed Description
_PrepParallelRestore plays a crucial role in enabling efficient parallel restore operations in PostgreSQL's custom dump format. The function calculates the dataLength field for TABLE DATA and BLOBS TOC entries by analyzing the positions of consecutive data items in the archive.

The function leverages the fact that data items are stored in TOC order within the archive file. It iterates through all TOC entries and computes each item's length as the difference between consecutive data positions. This size information is essential for the parallel restore scheduler to make informed decisions about job distribution and ordering.

For the last data item in the archive, the function can determine its length by seeking to the end of the file, provided the archive supports seeking operations. If seeking isn't available, some entries will remain without size estimates, but parallel restore can still function, albeit with potentially suboptimal scheduling.

## Parameters / Member Variables
- : ArchiveHandle pointer containing the archive context, TOC, and file handle

## Dependencies
- Functions called/Symbols referenced:
  - [lclContext](../l/lclContext.md) (local context structure type)
  - [TocEntry](../T/TocEntry.md) (table of contents entry structure)
  - lclTocEntry (local TOC entry extension)
  - K_OFFSET_POS_SET (constant indicating valid data position)
  - pgoff_t (PostgreSQL offset type)
  - fseeko (seeks to specific file position)
  - ftello (gets current file position)
- Called from (representative examples):
  - [InitArchiveFmt_Custom](../I/InitArchiveFmt_Custom.md) (custom format initialization)
  - lclTocEntry (directory format TOC entry handling)
  - [InitArchiveFmt_Directory](../I/InitArchiveFmt_Directory.md) (directory format initialization)

## Notes and Other Information
- This is a static function specific to the custom archive format implementation
- Located in src/bin/pg_dump/pg_backup_custom.c at lines 829-880
- Essential for optimal parallel restore performance
- Only processes TOC entries with valid data positions (K_OFFSET_POS_SET)
- Gracefully handles archives where TOC rewriting wasn't possible during creation
- The computed dataLength values guide the parallel restore job scheduler
- Requires seekable files for complete functionality but can work with limited information
- Part of the optional optimization interface for archive formats supporting parallel operations