# _Clone

## Location
[src/bin/pg_dump/pg_backup_custom.c:881-904](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_custom.c#L881-L904)

## Overview
A specialized function that creates thread-private working state during parallel restoration by cloning format-specific data structures for each worker thread.

## Definition


## Detailed Description
_Clone is essential for parallel restore operations in PostgreSQL's custom dump format. When multiple worker threads are processing the same archive, each thread needs its own private copy of the format-specific working state to avoid conflicts and data corruption.

The function creates a new lclContext structure for each thread by allocating memory and copying the original context. This ensures that each worker thread has its own independent working state while sharing the underlying archive data. The function includes a safety check to ensure no compression context is active during cloning, as active compressors would complicate the cloning process.

Importantly, the function intentionally does not clone TOC-entry-local state, allowing threads to share knowledge about data block locations. This sharing improves efficiency, but requires careful coordination in functions like _PrintTocData to manage concurrent access to shared state.

## Parameters / Member Variables
- : ArchiveHandle pointer containing the archive context and format data that needs to be cloned

## Dependencies
- Functions called/Symbols referenced:
  - [lclContext](../l/lclContext.md) (local context structure type)
  - pg_malloc (PostgreSQL memory allocation function)
  - memcpy (standard C library memory copy function)
- Called from (representative examples):
  - [InitArchiveFmt_Custom](../I/InitArchiveFmt_Custom.md) (custom format initialization)
  - lclTocEntry (directory format TOC entry handling)
  - [InitArchiveFmt_Directory](../I/InitArchiveFmt_Directory.md) (directory format initialization)

## Notes and Other Information
- This is a static function specific to the custom archive format implementation
- Located in src/bin/pg_dump/pg_backup_custom.c at lines 881-904
- Essential for thread safety during parallel restore operations
- Creates private working state while preserving shared TOC data
- Includes safety check for active compression contexts
- Part of the parallel operation interface for archive formats
- Each thread gets its own independent lclContext copy
- Shared TOC-entry-local state requires careful synchronization in other functions